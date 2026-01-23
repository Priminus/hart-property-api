import { Inject, Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import type { SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';

// URA API types - nested structure: Project -> Transaction[]
type UraTransactionDetail = {
  area: string; // SQM (square meters) - must convert to sqft
  floorRange: string;
  noOfUnits: string;
  contractDate: string;
  typeOfSale: string;
  price: string;
  propertyType: string;
  district: string;
  typeOfArea: string;
  tenure: string;
  nettPrice?: string;
};

type UraProject = {
  project: string;
  street: string;
  x: string;
  y: string;
  marketSegment: string;
  transaction: UraTransactionDetail[];
};

type FlatTransaction = {
  project: string;
  street: string;
  marketSegment: string;
} & UraTransactionDetail;

type UraApiResponse = {
  Status: string;
  Message: string;
  Result: UraProject[];
};

@Injectable()
export class UraService {
  private readonly logger = new Logger(UraService.name);

  constructor(
    @Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient,
  ) {}

  /**
   * Map URA typeOfSale code to human-readable string
   */
  private mapTypeOfSale(code: string): string | null {
    switch (code) {
      case '1':
        return 'New Sale';
      case '2':
        return 'Sub Sale';
      case '3':
        return 'Resale';
      default:
        return null;
    }
  }

  /**
   * Build case-insensitive condo name lookup map from existing DB data
   */
  private async buildCondoNameMap(): Promise<Map<string, string>> {
    const { data } = await this.supabase
      .from('condo_sale_transactions')
      .select('condo_name')
      .not('condo_name', 'is', null);

    const map = new Map<string, string>();
    if (data) {
      for (const row of data) {
        const name = row.condo_name as string;
        const lowerName = name.toLowerCase();
        if (!map.has(lowerName)) {
          map.set(lowerName, name);
        }
      }
    }
    return map;
  }

  /**
   * Get daily token from URA API
   */
  private async getToken(): Promise<string> {
    const apiKey = process.env.URA_API_KEY;
    if (!apiKey) throw new Error('URA_API_KEY not configured');

    const res = await fetch(
      'https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1',
      { headers: { AccessKey: apiKey } },
    );

    if (!res.ok) {
      throw new Error(`Failed to get URA token: ${res.status}`);
    }

    const data = (await res.json()) as { Status: string; Result: string };
    if (data.Status !== 'Success') {
      throw new Error(`URA token request failed: ${data.Status}`);
    }

    return data.Result;
  }

  /**
   * Fetch transactions from URA API for a specific batch
   * Flattens the nested Project -> Transaction[] structure
   */
  private async fetchBatch(
    token: string,
    batch: number,
  ): Promise<FlatTransaction[]> {
    const apiKey = process.env.URA_API_KEY;
    const url = `https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=PMI_Resi_Transaction&batch=${batch}`;

    const res = await fetch(url, {
      headers: { AccessKey: apiKey!, Token: token },
    });

    if (!res.ok) {
      throw new Error(`Failed to fetch URA batch ${batch}: ${res.status}`);
    }

    const data = (await res.json()) as UraApiResponse;
    if (data.Status !== 'Success') {
      this.logger.warn(`URA batch ${batch} returned status: ${data.Status}`);
      return [];
    }

    // Flatten: each project has multiple transactions
    const flat: FlatTransaction[] = [];
    for (const project of data.Result || []) {
      for (const txn of project.transaction || []) {
        flat.push({
          project: project.project,
          street: project.street,
          marketSegment: project.marketSegment,
          ...txn,
        });
      }
    }

    return flat;
  }

  /**
   * Parse URA contract date (MMYY) to YYYY-MM-DD
   */
  private parseContractDate(contractDate: string): string | null {
    if (!contractDate || contractDate.length !== 4) return null;

    const month = contractDate.substring(0, 2);
    const year = contractDate.substring(2, 4);
    const fullYear = parseInt(year, 10) >= 80 ? `19${year}` : `20${year}`;

    return `${fullYear}-${month}-15`;
  }

  /**
   * Parse floor range to level_low and level_high
   */
  private parseFloorRange(floorRange: string): {
    level_low: number | null;
    level_high: number | null;
  } {
    if (!floorRange) return { level_low: null, level_high: null };

    const match = floorRange.match(/(\d+)\s*-\s*(\d+)/);
    if (match) {
      return {
        level_low: parseInt(match[1], 10),
        level_high: parseInt(match[2], 10),
      };
    }

    const singleMatch = floorRange.match(/(\d+)/);
    if (singleMatch) {
      const level = parseInt(singleMatch[1], 10);
      return { level_low: level, level_high: level };
    }

    return { level_low: null, level_high: null };
  }

  /**
   * Transform URA transaction to DB row
   */
  private transformToDbRow(
    txn: FlatTransaction,
    condoNameMap: Map<string, string>,
  ): Record<string, unknown> | null {
    const salePrice = parseFloat(txn.price);
    if (!Number.isFinite(salePrice) || salePrice <= 0) return null;

    const rawCondoName = txn.project.trim();
    if (!rawCondoName) return null;

    const lowerName = rawCondoName.toLowerCase();
    const condoName = condoNameMap.get(lowerName) ?? rawCondoName;

    const saleDate = this.parseContractDate(txn.contractDate);
    const { level_low, level_high } = this.parseFloorRange(txn.floorRange);
    const typeOfSale = this.mapTypeOfSale(txn.typeOfSale);

    const sqm = txn.area ? parseFloat(txn.area) : null;
    const sqft = sqm ? Math.round(sqm * 10.764) : null;

    return {
      condo_name: condoName,
      unit_type: null,
      sqft,
      sale_date: saleDate,
      sale_price: salePrice,
      level_low,
      level_high,
      type_of_sale: typeOfSale,
      purchase_date: null,
      purchase_price: null,
      exact_level: null,
      exact_unit: null,
      profit: null,
      annualised_pct: null,
    };
  }

  /**
   * Bulk merge transactions using DB function
   * Merges with existing rows (fills in missing fields) or inserts new ones
   */
  private async bulkMerge(
    rows: Record<string, unknown>[],
  ): Promise<{ merged: number; inserted: number; errors: number }> {
    const CHUNK_SIZE = 500;
    let totalMerged = 0;
    let totalInserted = 0;
    let errors = 0;

    for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
      const chunk = rows.slice(i, i + CHUNK_SIZE);

      const { data, error } = await this.supabase.rpc(
        'merge_ura_transactions_batch',
        { transactions: chunk },
      );

      if (error) {
        this.logger.error(`Chunk error: ${error.message}`);
        errors += chunk.length;
      } else if (data) {
        totalMerged += (data as { merged: number; inserted: number }).merged || 0;
        totalInserted += (data as { merged: number; inserted: number }).inserted || 0;
      }
    }

    return { merged: totalMerged, inserted: totalInserted, errors };
  }

  /**
   * Run full ingestion from URA API
   * Deduplication is handled at DB level via unique index
   */
  async ingestTransactions(): Promise<{
    total: number;
    condos: number;
    inserted: number;
    skipped: number;
  }> {
    this.logger.log('Starting URA transaction ingestion...');
    this.logger.log('Deduplication handled at DB level via unique index');

    // Build condo name map for case-insensitive matching (to preserve existing casing)
    const condoNameMap = await this.buildCondoNameMap();
    this.logger.log(`Found ${condoNameMap.size} unique condo names in DB`);

    const token = await this.getToken();
    this.logger.log('URA token acquired');

    // Collect all rows
    const allRows: Record<string, unknown>[] = [];
    let total = 0;
    let totalCondos = 0;

    for (let batch = 1; batch <= 4; batch++) {
      this.logger.log(`Fetching batch ${batch}/4...`);

      const transactions = await this.fetchBatch(token, batch);
      total += transactions.length;

      const condoTxns = transactions.filter(
        (t) => t.propertyType === 'Condominium' || t.propertyType === 'Apartment',
      );
      this.logger.log(
        `Batch ${batch}: ${condoTxns.length}/${transactions.length} condos`,
      );
      totalCondos += condoTxns.length;

      for (const txn of condoTxns) {
        const row = this.transformToDbRow(txn, condoNameMap);
        if (row) allRows.push(row);
      }
    }

    this.logger.log(`Merging ${allRows.length} rows with existing data...`);

    const { merged, inserted, errors } = await this.bulkMerge(allRows);

    this.logger.log(
      `Ingestion complete: ${total} total, ${totalCondos} condos, ${merged} merged, ${inserted} inserted, ${errors} errors`,
    );

    return { total, condos: totalCondos, inserted, skipped: merged };
  }

  /**
   * Scheduled job: Run daily at 6 AM Singapore time
   * URA updates their data daily, so once per day is sufficient
   */
  @Cron(CronExpression.EVERY_DAY_AT_6AM, {
    name: 'ura-transaction-ingestion',
    timeZone: 'Asia/Singapore',
  })
  async handleScheduledIngestion(): Promise<void> {
    this.logger.log('Running scheduled URA ingestion...');
    try {
      await this.ingestTransactions();
    } catch (err) {
      this.logger.error(
        `Scheduled URA ingestion failed: ${err instanceof Error ? err.message : 'Unknown error'}`,
      );
    }
  }
}
