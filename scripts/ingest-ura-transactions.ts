/**
 * URA Property Transaction Ingestion Script
 *
 * Fetches private residential property transactions from URA API
 * and ingests Condominium transactions into condo_sale_transactions DB.
 *
 * Usage: npm run ingest:ura
 *
 * API Reference: https://eservice.ura.gov.sg/maps/api/#private-residential-property-transactions
 */

import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';

dotenv.config();

const URA_API_KEY = process.env.URA_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_API_KEY = process.env.SUPABASE_API_KEY;

if (!URA_API_KEY) throw new Error('URA_API_KEY not set in .env');
if (!SUPABASE_URL) throw new Error('SUPABASE_URL not set in .env');
if (!SUPABASE_API_KEY) throw new Error('SUPABASE_API_KEY not set in .env');

const supabase = createClient(SUPABASE_URL, SUPABASE_API_KEY);

/**
 * Map URA typeOfSale code to human-readable string
 */
function mapTypeOfSale(code: string): string | null {
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
 * Fetch all existing condo names from DB and build a case-insensitive lookup map
 * Returns: Map<lowercaseName, actualDbName>
 */
async function buildCondoNameMap(): Promise<Map<string, string>> {
  const { data } = await supabase
    .from('condo_sale_transactions')
    .select('condo_name')
    .not('condo_name', 'is', null);

  const map = new Map<string, string>();
  if (data) {
    for (const row of data) {
      const name = row.condo_name as string;
      const lowerName = name.toLowerCase();
      // Keep the first occurrence (which should be the manually entered one)
      if (!map.has(lowerName)) {
        map.set(lowerName, name);
      }
    }
  }
  return map;
}

// URA API types - nested structure: Project -> Transaction[]
type UraTransactionDetail = {
  area: string; // SQM (square meters) - must convert to sqft
  floorRange: string; // e.g. "06 - 10"
  noOfUnits: string;
  contractDate: string; // MMYY format
  typeOfSale: string; // "1" = New Sale, "2" = Sub Sale, "3" = Resale
  price: string; // Sale price
  propertyType: string; // "Condominium", "Apartment", etc.
  district: string;
  typeOfArea: string; // "Strata" or "Land"
  tenure: string;
  nettPrice?: string;
};

type UraProject = {
  project: string; // Condo/project name
  street: string;
  x: string; // SVY21 X coordinate
  y: string; // SVY21 Y coordinate
  marketSegment: string;
  transaction: UraTransactionDetail[];
};

// Flattened transaction for processing
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

/**
 * Get daily token from URA API
 */
async function getUraToken(): Promise<string> {
  const res = await fetch(
    'https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1',
    {
      headers: {
        AccessKey: URA_API_KEY!,
      },
    },
  );

  if (!res.ok) {
    throw new Error(`Failed to get URA token: ${res.status} ${res.statusText}`);
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
async function fetchUraBatch(
  token: string,
  batch: number,
): Promise<FlatTransaction[]> {
  const url = `https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=PMI_Resi_Transaction&batch=${batch}`;

  const res = await fetch(url, {
    headers: {
      AccessKey: URA_API_KEY!,
      Token: token,
    },
  });

  if (!res.ok) {
    throw new Error(
      `Failed to fetch URA batch ${batch}: ${res.status} ${res.statusText}`,
    );
  }

  const data = (await res.json()) as UraApiResponse;
  if (data.Status !== 'Success') {
    console.warn(`URA batch ${batch} returned status: ${data.Status}`);
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
 * Parse URA contract date (MMYY) to YYYY-MM-DD (use 15th of month)
 */
function parseContractDate(contractDate: string): string | null {
  if (!contractDate || contractDate.length !== 4) return null;

  const month = contractDate.substring(0, 2);
  const year = contractDate.substring(2, 4);

  // Assume 20xx for years 00-99
  const fullYear = parseInt(year, 10) >= 80 ? `19${year}` : `20${year}`;

  return `${fullYear}-${month}-15`;
}

/**
 * Parse floor range to level_low and level_high
 */
function parseFloorRange(
  floorRange: string,
): { level_low: number | null; level_high: number | null } {
  if (!floorRange) return { level_low: null, level_high: null };

  // Format is typically "06 - 10" or "01 - 05"
  const match = floorRange.match(/(\d+)\s*-\s*(\d+)/);
  if (match) {
    return {
      level_low: parseInt(match[1], 10),
      level_high: parseInt(match[2], 10),
    };
  }

  // Single floor number
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
function transformToDbRow(
  txn: FlatTransaction,
  condoNameMap: Map<string, string>,
): Record<string, unknown> | null {
  const salePrice = parseFloat(txn.price);
  if (!Number.isFinite(salePrice) || salePrice <= 0) return null;

  const rawCondoName = txn.project.trim();
  if (!rawCondoName) return null;

  const lowerName = rawCondoName.toLowerCase();
  const condoName = condoNameMap.get(lowerName) ?? rawCondoName;

  const saleDate = parseContractDate(txn.contractDate);
  const { level_low, level_high } = parseFloorRange(txn.floorRange);
  const typeOfSale = mapTypeOfSale(txn.typeOfSale);

  // URA returns area in SQM (square meters) - convert to sqft
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
async function bulkMergeRows(
  rows: Record<string, unknown>[],
): Promise<{ merged: number; inserted: number; errors: number }> {
  const CHUNK_SIZE = 500;
  let totalMerged = 0;
  let totalInserted = 0;
  let errors = 0;

  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);

    // Call the batch merge function via RPC
    const { data, error } = await supabase.rpc('merge_ura_transactions_batch', {
      transactions: chunk,
    });

    if (error) {
      console.error(`Chunk ${Math.floor(i / CHUNK_SIZE) + 1} error: ${error.message}`);
      errors += chunk.length;
    } else if (data) {
      totalMerged += data.merged || 0;
      totalInserted += data.inserted || 0;
    }

    console.log(`  Processed ${Math.min(i + CHUNK_SIZE, rows.length)}/${rows.length} rows...`);
  }

  return { merged: totalMerged, inserted: totalInserted, errors };
}

/**
 * Main ingestion function
 * Deduplication is handled at DB level via unique index on (LOWER(condo_name), sale_price, sale_month)
 */
async function ingestUraTransactions(): Promise<void> {
  console.log('Starting URA transaction ingestion...');
  console.log('Deduplication handled at DB level via unique index.');

  // Build condo name map for case-insensitive matching (to preserve existing casing)
  console.log('Building condo name map from existing DB data...');
  const condoNameMap = await buildCondoNameMap();
  console.log(`Found ${condoNameMap.size} unique condo names in DB.`);

  console.log('Fetching URA API token...');
  const token = await getUraToken();
  console.log('Token acquired.');

  // Collect all rows to insert
  const allRows: Record<string, unknown>[] = [];
  let totalFromApi = 0;
  let totalCondos = 0;

  for (let batch = 1; batch <= 4; batch++) {
    console.log(`\nFetching batch ${batch}/4...`);

    const transactions = await fetchUraBatch(token, batch);
    totalFromApi += transactions.length;
    console.log(`Batch ${batch}: ${transactions.length} transactions`);

    // Filter for Condominium and Apartment (both are private residential)
    const condoTxns = transactions.filter(
      (txn) => txn.propertyType === 'Condominium' || txn.propertyType === 'Apartment',
    );
    console.log(`Batch ${batch}: ${condoTxns.length} condominium transactions`);
    totalCondos += condoTxns.length;

    for (const txn of condoTxns) {
      const row = transformToDbRow(txn, condoNameMap);
      if (row) allRows.push(row);
    }
  }

  console.log(`\n=== Merging ${allRows.length} rows with existing data ===`);

  const { merged, inserted, errors } = await bulkMergeRows(allRows);

  console.log('\n=== Ingestion Complete ===');
  console.log(`Total transactions from URA API: ${totalFromApi}`);
  console.log(`Condominium transactions: ${totalCondos}`);
  console.log(`Merged with existing: ${merged}`);
  console.log(`Inserted new: ${inserted}`);
  if (errors > 0) console.log(`Errors: ${errors}`);
}

// Run if called directly
ingestUraTransactions()
  .then(() => {
    console.log('Done.');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Ingestion failed:', err);
    process.exit(1);
  });
