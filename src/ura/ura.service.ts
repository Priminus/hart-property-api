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
      .from('sale_transactions')
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
   * Helper to fetch with retry logic
   */
  private async fetchWithRetry(
    url: string,
    options: RequestInit,
    retries = 3,
    backoff = 2000,
  ): Promise<Response> {
    for (let i = 0; i < retries; i++) {
      try {
        const res = await fetch(url, options);
        if (res.ok) return res;
        
        // If it's a rate limit or server error, we retry
        if (res.status === 429 || res.status >= 500) {
          this.logger.warn(`Fetch failed with status ${res.status}. Retry ${i + 1}/${retries}...`);
        } else {
          // Other errors might not benefit from retry, but let's be safe
          this.logger.warn(`Fetch failed with status ${res.status}. Retry ${i + 1}/${retries}...`);
        }
      } catch (err) {
        this.logger.error(`Fetch attempt ${i + 1} failed: ${err instanceof Error ? err.message : String(err)}`);
        if (i === retries - 1) throw err;
      }
      
      // Wait before retry
      await new Promise(resolve => setTimeout(resolve, backoff * (i + 1)));
    }
    throw new Error(`Failed to fetch after ${retries} retries: ${url}`);
  }

  /**
   * Get daily token from URA API
   */
  private async getToken(): Promise<string> {
    const apiKey = process.env.URA_API_KEY;
    if (!apiKey) throw new Error('URA_API_KEY not configured');

    const res = await this.fetchWithRetry(
      'https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1',
      { headers: { AccessKey: apiKey } },
    );

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

    const res = await this.fetchWithRetry(url, {
      headers: { AccessKey: apiKey!, Token: token },
    });

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
      property_type: txn.propertyType,
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
    residential: number;
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
    let totalResidential = 0;

    const VALID_PROPERTY_TYPES = [
      'Strata Detached',
      'Strata Semidetached',
      'Strata Terrace',
      'Detached',
      'Semi-detached',
      'Terrace',
      'Apartment',
      'Condominium',
      'Executive Condominium',
    ];

    for (let batch = 1; batch <= 4; batch++) {
      this.logger.log(`Fetching batch ${batch}/4...`);

      const transactions = await this.fetchBatch(token, batch);
      total += transactions.length;

      const residentialTxns = transactions.filter((t) =>
        VALID_PROPERTY_TYPES.includes(t.propertyType),
      );
      this.logger.log(
        `Batch ${batch}: ${residentialTxns.length}/${transactions.length} residential properties`,
      );
      totalResidential += residentialTxns.length;

      for (const txn of residentialTxns) {
        const row = this.transformToDbRow(txn, condoNameMap);
        if (row) allRows.push(row);
      }
    }

    this.logger.log(`Merging ${allRows.length} rows with existing data...`);

    const { merged, inserted, errors } = await this.bulkMerge(allRows);

    this.logger.log(
      `Ingestion complete: ${total} total, ${totalResidential} residential, ${merged} merged, ${inserted} inserted, ${errors} errors`,
    );

    return { total, residential: totalResidential, inserted, skipped: merged };
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
