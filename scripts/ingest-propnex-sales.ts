import fs from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_API_KEY = process.env.SUPABASE_API_KEY;
const PROPNEX_BEARER_TOKEN = process.env.PROPNEX_BEARER_TOKEN;

if (!SUPABASE_URL) throw new Error('SUPABASE_URL not set in .env');
if (!SUPABASE_API_KEY) throw new Error('SUPABASE_API_KEY not set in .env');
if (!PROPNEX_BEARER_TOKEN) {
  throw new Error('PROPNEX_BEARER_TOKEN not set in .env');
}

const PROJECT_ID_START = 2082;
const PROJECT_ID_END = 2082;
const MISSING_CSV = path.resolve(__dirname, '../missing.csv');

const supabase = createClient(SUPABASE_URL, SUPABASE_API_KEY);

type PropnexSale = {
  saleId?: number | string | null;
  saleProjectName?: string | null;
  saleProjectNameDisplay?: string | null;
  saleDate?: string | null;
  salePrice?: number | string | null;
  saleProfit?: number | string | null;
  saleReturnAnnualized?: number | string | null;
  saleHoldingDays?: number | string | null;
  saleAreaSqft?: number | string | null;
  saleFloor?: number | string | null;
  saleUnitNum?: number | string | null;
  unitType?: string | null;
  saleType?: string | null;
  typeName?: string | null;
  saleSubtype?: string | null;
  towerFloor?: number | string | null;
  towerUnitNum?: number | string | null;
  towerAreaSqft?: number | string | null;
  purchaseDate?: string | null;
  purchasePrice?: number | string | null;
};

type PropnexRental = {
  rentalId?: number | string | null;
  rentalProjectName?: string | null;
  rentalProjectNameDisplay?: string | null;
  rentalStreet?: string | null;
  rentalStreetDisplay?: string | null;
  rentalLeaseDate?: string | null;
  rentalPropertyType?: string | null;
  rentalAreaSqm?: string | null;
  rentalAreaSqft?: string | null;
  rentalAreaSqftMin?: number | string | null;
  rentalAreaSqftMax?: number | string | null;
  rentalRent?: number | string | null;
  rentalPsf?: number | string | null;
  rentalBedroom?: number | string | null;
  projectId?: number | string | null;
  addressId?: number | string | null;
  districtId?: number | string | null;
  subtypeId?: number | string | null;
  typeId?: number | string | null;
  realisId?: number | string | null;
  typeName?: string | null;
  typeCategory?: string | null;
  typeGuruGroup?: string | null;
  typeGuru?: string | null;
  typeHdb?: string | null;
  typeHdbName?: string | null;
  typeHdbRental?: number | string | null;
  parentId?: number | string | null;
  isClusterHouse?: number | string | null;
};

type PropnexResponse = {
  districtId?: number | string | null;
  typeId?: number | string | null;
  projectId?: number | string | null;
  projectName?: string | null;
  projectNameDisplay?: string | null;
  projectDeveloper?: string | null;
  projectLatitude?: number | string | null;
  projectLongitude?: number | string | null;
  projectStreetDisplay?: string | null;
  projectRegion?: string | null;
  projectSubtown?: string | null;
  projectNumUnits?: number | string | null;
  projectTenureFrom?: string | null;
  projectTenures?: string | null;
  projectBedroomTypes?: string | null;
  projectSubtypeIds?: string | null;
  projectAreaSqftMin?: number | string | null;
  projectAreaSqftMax?: number | string | null;
  projectMaxFloorLvl?: number | string | null;
  projectCompletionYear?: number | string | null;
  projectPlotRatio?: number | string | null;
  projectLandSizeSqm?: number | string | null;
  projectGfaSqm?: number | string | null;
  projectMcst?: string | null;
  projectPhotoUrl?: string | null;
  projectNew?: string | null;
  projectNewTOP?: string | null;
  projectNewZeroVolume?: string | null;
  projectSource?: string | null;
  projectNA?: number | string | null;
  projectModifiedDateTime?: string | null;
  projectDeleted?: number | string | null;
  estateId?: number | string | null;
  typeName?: string | null;
  typeCategory?: string | null;
  typeGuruGroup?: string | null;
  typeGuru?: string | null;
  typeHdb?: string | null;
  typeHdbName?: string | null;
  typeHdbRental?: number | string | null;
  parentId?: number | string | null;
  districtLocation?: string | null;
  sales?: PropnexSale[];
  profitSales?: PropnexSale[];
  lossSales?: PropnexSale[];
  rentals?: PropnexRental[];
};

type SaleRow = {
  id?: string;
  condo_name: string;
  condo_name_lower: string;
  propnex_project_id: number | null;
  propnex_project_name: string | null;
  sale_date: string;
  sale_price: number;
  sale_month: number;
  sqft: number | null;
  unit_type: string | null;
  exact_level: number | null;
  exact_unit: number | null;
  type_of_sale: string | null;
  property_type: string | null;
  purchase_price: number | null;
  purchase_date: string | null;
  profit: number | null;
  annualised_pct: number | null;
};

type PropnexProjectRow = {
  project_id: number;
  district_id: number | null;
  type_id: number | null;
  project_name: string | null;
  project_name_display: string | null;
  project_developer: string | null;
  project_latitude: number | null;
  project_longitude: number | null;
  project_street_display: string | null;
  project_region: string | null;
  project_subtown: string | null;
  project_num_units: number | null;
  project_tenure_from: string | null;
  project_tenures: unknown;
  project_bedroom_types: unknown;
  project_subtype_ids: unknown;
  project_area_sqft_min: number | null;
  project_area_sqft_max: number | null;
  project_max_floor_lvl: number | null;
  project_completion_year: number | null;
  project_plot_ratio: number | null;
  project_land_size_sqm: number | null;
  project_gfa_sqm: number | null;
  project_mcst: string | null;
  project_photo_url: string | null;
  project_new: string | null;
  project_new_top: string | null;
  project_new_zero_volume: string | null;
  project_source: string | null;
  project_na: number | null;
  project_modified_date_time: string | null;
  project_deleted: number | null;
  estate_id: number | null;
  type_name: string | null;
  type_category: string | null;
  type_guru_group: string | null;
  type_guru: string | null;
  type_hdb: string | null;
  type_hdb_name: string | null;
  type_hdb_rental: number | null;
  parent_id: number | null;
  district_location: string | null;
};

type RentalRow = {
  rental_id: number;
  project_id: number | null;
  rental_project_name: string | null;
  rental_project_name_display: string | null;
  rental_street: string | null;
  rental_street_display: string | null;
  rental_lease_date: string | null;
  rental_property_type: string | null;
  rental_area_sqm: string | null;
  rental_area_sqft: string | null;
  rental_area_sqft_min: number | null;
  rental_area_sqft_max: number | null;
  rental_rent: number | null;
  rental_psf: number | null;
  rental_bedroom: number | null;
  address_id: number | null;
  district_id: number | null;
  subtype_id: number | null;
  type_id: number | null;
  realis_id: number | null;
  type_name: string | null;
  type_category: string | null;
  type_guru_group: string | null;
  type_guru: unknown;
  type_hdb: string | null;
  type_hdb_name: string | null;
  type_hdb_rental: number | null;
  parent_id: number | null;
  is_cluster_house: number | null;
};

function toDateString(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date.getTime());
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function computeSaleMonth(dateStr: string): number {
  return Number.parseInt(dateStr.substring(0, 4) + dateStr.substring(5, 7), 10);
}

function parseNumber(value: number | string | null | undefined): number | null {
  if (value == null) return null;
  const num = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(num) ? num : null;
}

function parseInteger(
  value: number | string | null | undefined,
): number | null {
  if (value == null) return null;
  const num = Number.parseInt(String(value), 10);
  return Number.isFinite(num) ? num : null;
}

function parseNonZeroInteger(
  value: number | string | null | undefined,
): number | null {
  const num = parseInteger(value);
  if (num == null || num === 0) return null;
  return num;
}

function parseJsonField(value: string | null | undefined): unknown {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed.startsWith('[') && !trimmed.startsWith('{')) return null;
  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

function coalesceNonNull<T>(values: Array<T | null | undefined>): T | null {
  for (const value of values) {
    if (value != null) return value;
  }
  return null;
}

function computePurchaseDate(
  saleDate: string,
  holdingDays: number | null,
): string | null {
  if (!holdingDays || holdingDays <= 0) return null;
  const base = new Date(`${saleDate}T00:00:00Z`);
  if (!Number.isFinite(base.getTime())) return null;
  return toDateString(addDays(base, -holdingDays));
}

async function fetchProjectData(
  projectId: number,
): Promise<PropnexResponse | null> {
  const endDate = toDateString(new Date());
  const start = new Date();
  start.setUTCFullYear(start.getUTCFullYear() - 10);
  const startDate = toDateString(start);

  const body = new URLSearchParams({
    dateRangeType: '10Y',
    distance: '0',
    endDate,
    soreal: '0',
    startDate,
  });

  const res = await fetch(
    `https://investment-production.propnex.net/v1/analysis/project/${projectId}`,
    {
      method: 'POST',
      headers: {
        Host: 'investment-production.propnex.net',
        Accept: '*/*',
        Connection: 'keep-alive',
        'User-Agent':
          'InvestmentSuite2/3.3.23 (com.propNex.InvestorSuite; build:371; iOS 18.7.0) Alamofire/5.9.0',
        'Accept-Language': 'en-SG;q=1.0, zh-Hans-SG;q=0.9',
        Authorization: `Bearer ${PROPNEX_BEARER_TOKEN}`,
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
      },
      body,
    },
  );

  if (!res.ok) {
    console.warn(`PropNex API error for ${projectId}: ${res.status}`);
    return null;
  }

  const text = await res.text();
  if (text.includes('Slim Application Error')) {
    console.log(`Slim Application Error for project ${projectId}. Stopping.`);
    process.exit(1);
  }
  try {
    return JSON.parse(text) as PropnexResponse;
  } catch {
    console.warn(`PropNex API non-JSON response for ${projectId}`);
    return null;
  }
}

async function upsertProject(
  data: PropnexResponse,
  projectId: number,
): Promise<PropnexProjectRow | null> {
  const resolvedProjectId = parseInteger(data.projectId) ?? projectId;
  if (!resolvedProjectId) return null;

  const row: PropnexProjectRow = {
    project_id: resolvedProjectId,
    district_id: parseInteger(data.districtId),
    type_id: parseInteger(data.typeId),
    project_name: data.projectName ?? null,
    project_name_display: data.projectNameDisplay ?? null,
    project_developer: data.projectDeveloper ?? null,
    project_latitude: parseNumber(data.projectLatitude),
    project_longitude: parseNumber(data.projectLongitude),
    project_street_display: data.projectStreetDisplay ?? null,
    project_region: data.projectRegion ?? null,
    project_subtown: data.projectSubtown ?? null,
    project_num_units: parseInteger(data.projectNumUnits),
    project_tenure_from: data.projectTenureFrom ?? null,
    project_tenures: parseJsonField(data.projectTenures),
    project_bedroom_types: parseJsonField(data.projectBedroomTypes),
    project_subtype_ids: parseJsonField(data.projectSubtypeIds),
    project_area_sqft_min: parseNumber(data.projectAreaSqftMin),
    project_area_sqft_max: parseNumber(data.projectAreaSqftMax),
    project_max_floor_lvl: parseInteger(data.projectMaxFloorLvl),
    project_completion_year: parseInteger(data.projectCompletionYear),
    project_plot_ratio: parseNumber(data.projectPlotRatio),
    project_land_size_sqm: parseNumber(data.projectLandSizeSqm),
    project_gfa_sqm: parseNumber(data.projectGfaSqm),
    project_mcst: data.projectMcst ?? null,
    project_photo_url: data.projectPhotoUrl ?? null,
    project_new: data.projectNew ?? null,
    project_new_top: data.projectNewTOP ?? null,
    project_new_zero_volume: data.projectNewZeroVolume ?? null,
    project_source: data.projectSource ?? null,
    project_na: parseInteger(data.projectNA),
    project_modified_date_time: data.projectModifiedDateTime ?? null,
    project_deleted: parseInteger(data.projectDeleted),
    estate_id: parseInteger(data.estateId),
    type_name: data.typeName ?? null,
    type_category: data.typeCategory ?? null,
    type_guru_group: data.typeGuruGroup ?? null,
    type_guru: data.typeGuru ?? null,
    type_hdb: data.typeHdb ?? null,
    type_hdb_name: data.typeHdbName ?? null,
    type_hdb_rental: parseInteger(data.typeHdbRental),
    parent_id: parseInteger(data.parentId),
    district_location: data.districtLocation ?? null,
  };

  const { error } = await supabase.from('propnex_projects').upsert(row, {
    onConflict: 'project_id',
  });
  if (error) {
    throw new Error(`Project upsert failed: ${error.message}`);
  }
  return row;
}

async function appendMissingProject(projectId: number): Promise<void> {
  await fs.appendFile(MISSING_CSV, `${projectId}\n`);
}

async function ingestRentals(rentals: PropnexRental[]): Promise<void> {
  const rows: RentalRow[] = [];
  for (const rental of rentals) {
    const rentalId = parseInteger(rental.rentalId);
    if (!rentalId) continue;

    rows.push({
      rental_id: rentalId,
      project_id: parseInteger(rental.projectId),
      rental_project_name: rental.rentalProjectName ?? null,
      rental_project_name_display: rental.rentalProjectNameDisplay ?? null,
      rental_street: rental.rentalStreet ?? null,
      rental_street_display: rental.rentalStreetDisplay ?? null,
      rental_lease_date: rental.rentalLeaseDate ?? null,
      rental_property_type: rental.rentalPropertyType ?? null,
      rental_area_sqm: rental.rentalAreaSqm ?? null,
      rental_area_sqft: rental.rentalAreaSqft ?? null,
      rental_area_sqft_min: parseNumber(rental.rentalAreaSqftMin),
      rental_area_sqft_max: parseNumber(rental.rentalAreaSqftMax),
      rental_rent: parseNumber(rental.rentalRent),
      rental_psf: parseNumber(rental.rentalPsf),
      rental_bedroom: parseInteger(rental.rentalBedroom),
      address_id: parseInteger(rental.addressId),
      district_id: parseInteger(rental.districtId),
      subtype_id: parseInteger(rental.subtypeId),
      type_id: parseInteger(rental.typeId),
      realis_id: parseInteger(rental.realisId),
      type_name: rental.typeName ?? null,
      type_category: rental.typeCategory ?? null,
      type_guru_group: rental.typeGuruGroup ?? null,
      type_guru: parseJsonField(rental.typeGuru),
      type_hdb: rental.typeHdb ?? null,
      type_hdb_name: rental.typeHdbName ?? null,
      type_hdb_rental: parseInteger(rental.typeHdbRental),
      parent_id: parseInteger(rental.parentId),
      is_cluster_house: parseInteger(rental.isClusterHouse),
    });
  }

  if (!rows.length) {
    console.log('No rentals to ingest.');
    return;
  }

  const CHUNK_SIZE = 200;
  let success = 0;
  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);
    const { error } = await supabase.from('rental_transactions').upsert(chunk, {
      onConflict: 'rental_id',
    });
    if (error) {
      console.error(
        `Rentals chunk ${Math.floor(i / CHUNK_SIZE) + 1} error:`,
        error.message,
      );
    } else {
      success += chunk.length;
    }
  }

  console.log(`Ingested ${success}/${rows.length} rental rows.`);
}

function normalizeExistingRow(raw: Record<string, unknown>): SaleRow {
  const saleDate = typeof raw.sale_date === 'string' ? raw.sale_date : '';
  const condoName = typeof raw.condo_name === 'string' ? raw.condo_name : '';
  const condoLower =
    typeof raw.condo_name_lower === 'string'
      ? raw.condo_name_lower
      : condoName.toLowerCase();
  const salePrice = parseNumber(raw.sale_price as number | string | null) ?? 0;
  return {
    id: typeof raw.id === 'string' ? raw.id : undefined,
    condo_name: condoName,
    condo_name_lower: condoLower,
    propnex_project_id: parseInteger(
      raw.propnex_project_id as number | string | null,
    ),
    propnex_project_name:
      typeof raw.propnex_project_name === 'string'
        ? raw.propnex_project_name
        : null,
    sale_date: saleDate,
    sale_price: salePrice,
    sale_month: computeSaleMonth(saleDate),
    sqft: parseNumber(raw.sqft as number | string | null),
    unit_type: typeof raw.unit_type === 'string' ? raw.unit_type : null,
    exact_level: parseInteger(raw.exact_level as number | string | null),
    exact_unit: parseInteger(raw.exact_unit as number | string | null),
    type_of_sale:
      typeof raw.type_of_sale === 'string' ? raw.type_of_sale : null,
    property_type:
      typeof raw.property_type === 'string' ? raw.property_type : null,
    purchase_price: parseNumber(raw.purchase_price as number | string | null),
    purchase_date:
      typeof raw.purchase_date === 'string' ? raw.purchase_date : null,
    profit: parseNumber(raw.profit as number | string | null),
    annualised_pct: parseNumber(raw.annualised_pct as number | string | null),
  };
}

function mergeRows(existingRows: SaleRow[], incomingRows: SaleRow[]): SaleRow {
  const allRows = [...existingRows, ...incomingRows];
  const sample = allRows[0];

  const merged: SaleRow = {
    condo_name:
      coalesceNonNull(allRows.map((r) => r.condo_name)) ?? sample.condo_name,
    condo_name_lower: sample.condo_name_lower,
    propnex_project_id: coalesceNonNull(
      allRows.map((r) => r.propnex_project_id),
    ),
    propnex_project_name: coalesceNonNull(
      allRows.map((r) => r.propnex_project_name),
    ),
    sale_date: sample.sale_date,
    sale_price: sample.sale_price,
    sale_month: computeSaleMonth(sample.sale_date),
    property_type: coalesceNonNull(allRows.map((r) => r.property_type)),
    type_of_sale: coalesceNonNull(allRows.map((r) => r.type_of_sale)),
    purchase_price: coalesceNonNull(allRows.map((r) => r.purchase_price)),
    purchase_date: coalesceNonNull(allRows.map((r) => r.purchase_date)),
    profit: coalesceNonNull(allRows.map((r) => r.profit)),
    annualised_pct: coalesceNonNull(allRows.map((r) => r.annualised_pct)),
    unit_type: coalesceNonNull(allRows.map((r) => r.unit_type)),
    sqft: coalesceNonNull(allRows.map((r) => r.sqft)),
    exact_level: coalesceNonNull(allRows.map((r) => r.exact_level)),
    exact_unit: coalesceNonNull(allRows.map((r) => r.exact_unit)),
  };

  return merged;
}

function isMissingPositiveInt(value: number | null): boolean {
  return value == null || value <= 0;
}

function isMissingPositiveNumber(value: number | null): boolean {
  return value == null || !Number.isFinite(value) || value <= 0;
}

function hasMissingCoreFields(row: SaleRow): boolean {
  return (
    isMissingPositiveInt(row.exact_level) ||
    isMissingPositiveInt(row.exact_unit) ||
    row.unit_type == null ||
    row.purchase_date == null ||
    isMissingPositiveNumber(row.purchase_price)
  );
}

async function ingestProject(projectId: number): Promise<void> {
  console.log(`Fetching PropNex data for project ${projectId}...`);
  const response = await fetchProjectData(projectId);
  if (!response) {
    await appendMissingProject(projectId);
    return;
  }

  const project = await upsertProject(response, projectId);
  const sales = response.sales ?? [];
  const profitSales = response.profitSales ?? [];
  const lossSales = response.lossSales ?? [];
  const rentals = response.rentals ?? [];
  const salesById = new Map<number, PropnexSale>();
  for (const sale of sales) {
    const id = parseInteger(sale.saleId);
    if (id != null) salesById.set(id, sale);
  }
  for (const sale of [...profitSales, ...lossSales]) {
    const id = parseInteger(sale.saleId);
    if (id != null) {
      salesById.set(id, { ...salesById.get(id), ...sale });
    }
  }
  const combinedSales = Array.from(salesById.values());
  console.log(
    `Fetched ${sales.length} sales rows, ${profitSales.length} profit sales, ${lossSales.length} loss sales.`,
  );
  console.log(`Fetched ${rentals.length} rental rows.`);

  await ingestRentals(rentals);

  const rows: SaleRow[] = [];
  for (const sale of combinedSales) {
    const condoName =
      sale.saleProjectNameDisplay?.trim() ||
      sale.saleProjectName?.trim() ||
      project?.project_name_display ||
      project?.project_name ||
      '';
    if (!condoName) continue;

    const saleDate = sale.saleDate ? sale.saleDate.substring(0, 10) : null;
    const salePrice = parseNumber(sale.salePrice);
    const exactLevel =
      parseNonZeroInteger(sale.saleFloor) ??
      parseNonZeroInteger(sale.towerFloor);
    const exactUnit =
      parseNonZeroInteger(sale.saleUnitNum) ??
      parseNonZeroInteger(sale.towerUnitNum);
    if (!saleDate || !salePrice) continue;

    const saleMonth = computeSaleMonth(saleDate);
    const profit = parseNumber(sale.saleProfit);
    const annualisedRaw = parseNumber(sale.saleReturnAnnualized);
    const annualisedPct =
      annualisedRaw == null ? null : Math.round(annualisedRaw * 10000) / 100;
    const holdingDays = parseInteger(sale.saleHoldingDays);
    const purchaseDate =
      sale.purchaseDate ??
      (holdingDays != null ? computePurchaseDate(saleDate, holdingDays) : null);
    const purchasePrice =
      parseNumber(sale.purchasePrice) ??
      (profit == null ? null : Math.round((salePrice - profit) * 100) / 100);

    const sqft =
      parseNumber(sale.saleAreaSqft) ?? parseNumber(sale.towerAreaSqft);

    const row: SaleRow = {
      condo_name: condoName,
      condo_name_lower: condoName.toLowerCase(),
      propnex_project_id: project?.project_id ?? projectId,
      propnex_project_name:
        project?.project_name_display ?? project?.project_name ?? null,
      sale_date: saleDate,
      sale_price: salePrice,
      sale_month: saleMonth,
      sqft: sqft,
      unit_type: sale.unitType ?? null,
      exact_level: exactLevel,
      exact_unit: exactUnit,
      type_of_sale: sale.saleType ?? null,
      property_type: sale.typeName ?? sale.saleSubtype ?? null,
      purchase_price: purchasePrice,
      purchase_date: purchaseDate,
      profit: profit,
      annualised_pct: annualisedPct,
    };
    rows.push(row);
  }

  console.log(`Prepared ${rows.length} sales rows to ingest.`);
  if (!rows.length) return;

  const grouped = new Map<string, SaleRow[]>();
  for (const row of rows) {
    const key = `${row.condo_name_lower}::${row.sale_date}::${row.sale_price}`;
    const list = grouped.get(key) ?? [];
    list.push(row);
    grouped.set(key, list);
  }

  const keys = Array.from(grouped.keys());
  const existingByKey = new Map<string, SaleRow[]>();
  const QUERY_CHUNK_SIZE = 200;

  for (let i = 0; i < keys.length; i += QUERY_CHUNK_SIZE) {
    const chunkKeys = keys.slice(i, i + QUERY_CHUNK_SIZE);
    const condoSet = new Set<string>();
    const dateSet = new Set<string>();
    const priceSet = new Set<number>();

    for (const key of chunkKeys) {
      const sample = grouped.get(key)?.[0];
      if (!sample) continue;
      condoSet.add(sample.condo_name_lower);
      dateSet.add(sample.sale_date);
      priceSet.add(sample.sale_price);
    }

    const { data, error } = await supabase
      .from('sale_transactions')
      .select(
        'id, condo_name, condo_name_lower, propnex_project_id, propnex_project_name, sale_date, sale_price, sale_month, sqft, unit_type, exact_level, exact_unit, type_of_sale, property_type, purchase_price, purchase_date, profit, annualised_pct',
      )
      .in('condo_name_lower', Array.from(condoSet))
      .in('sale_date', Array.from(dateSet))
      .in('sale_price', Array.from(priceSet));
    if (error) {
      console.error('Fetch existing rows error:', error.message);
      continue;
    }

    for (const row of data ?? []) {
      const normalized = normalizeExistingRow(row as Record<string, unknown>);
      const key = `${normalized.condo_name_lower}::${normalized.sale_date}::${normalized.sale_price}`;
      const list = existingByKey.get(key) ?? [];
      list.push(normalized);
      existingByKey.set(key, list);
    }
  }

  const toInsert: SaleRow[] = [];
  const toUpdate: Array<{ ids: string[]; row: SaleRow }> = [];
  let skipped = 0;
  for (const [key, groupRows] of grouped) {
    const existingRows = existingByKey.get(key) ?? [];
    if (existingRows.length > 0) {
      const matchingMissing = existingRows.filter(hasMissingCoreFields);
      if (!matchingMissing.length) {
        skipped += 1;
        continue;
      }

      const ids = matchingMissing
        .map((row) => row.id)
        .filter((id): id is string => typeof id === 'string');
      if (!ids.length) {
        skipped += 1;
        continue;
      }

      const merged = mergeRows(matchingMissing, groupRows);
      toUpdate.push({ ids, row: merged });
    } else {
      toInsert.push(mergeRows([], groupRows));
    }
  }

  const UPSERT_CHUNK_SIZE = 200;
  const exactRows = toInsert.filter(
    (row) => row.exact_level != null && row.exact_unit != null,
  );
  const uraRows = toInsert.filter(
    (row) => row.exact_level == null && row.exact_unit == null,
  );

  let success = 0;
  for (const { label, rows, onConflict, mode } of [
    {
      label: 'exact',
      rows: exactRows,
      onConflict:
        'condo_name_lower,sale_price,sale_month,exact_level,exact_unit',
      mode: 'upsert',
    },
    {
      label: 'ura',
      rows: uraRows,
      onConflict: null,
      mode: 'insert',
    },
  ]) {
    for (let i = 0; i < rows.length; i += UPSERT_CHUNK_SIZE) {
      const chunk = rows.slice(i, i + UPSERT_CHUNK_SIZE);
      const { error } =
        mode === 'upsert'
          ? await supabase.from('sale_transactions').upsert(chunk, {
              onConflict: String(onConflict),
            })
          : await supabase.from('sale_transactions').insert(chunk);
      if (error) {
        console.error(
          `Upsert ${label} chunk ${Math.floor(i / UPSERT_CHUNK_SIZE) + 1} error:`,
          error.message,
        );
        continue;
      }
      success += chunk.length;
    }
  }

  for (const update of toUpdate) {
    const { error } = await supabase
      .from('sale_transactions')
      .update(update.row)
      .in('id', update.ids)
      .or('exact_level.is.null,purchase_date.is.null,unit_type.is.null');
    if (error) {
      console.error('Update error:', error.message);
      continue;
    }
    success += update.ids.length;
  }

  console.log(
    `Ingested ${success}/${grouped.size} merged keys. Skipped ${skipped}.`,
  );

  const { error: cleanupError } = await supabase
    .from('sale_transactions')
    .update({ exact_level: null, exact_unit: null })
    .eq('propnex_project_id', projectId)
    .is('purchase_price', null)
    .not('exact_unit', 'is', null);
  if (cleanupError) {
    console.error(
      `Cleanup missing purchase_price for project ${projectId} failed:`,
      cleanupError.message,
    );
  }
}

async function ingestRange(): Promise<void> {
  for (
    let projectId = PROJECT_ID_START;
    projectId <= PROJECT_ID_END;
    projectId += 1
  ) {
    await ingestProject(projectId);
  }
}

ingestRange()
  .then(() => {
    console.log('Done.');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Ingestion failed:', err);
    process.exit(1);
  });
