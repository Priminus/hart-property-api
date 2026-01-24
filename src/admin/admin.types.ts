export type SaleTransaction = {
  id: string;
  condo_name: string;
  property_type: string | null;
  unit_type: string;
  sqft: number | null;
  exact_level: number | null;
  // NOTE: exact_unit is intentionally NEVER exposed via API
  level_low: number | null;
  level_high: number | null;
  purchase_date: string | null; // YYYY-MM-DD
  purchase_price: number | null;
  sale_date: string | null; // YYYY-MM-DD
  sale_price: number | null;
  profit: number | null;
  annualised_pct: number | null;
  created_at: string;
};

export type ListSaleTransactionsResponse =
  | {
      ok: true;
      rows: SaleTransaction[];
      total_count: number;
      limit: number;
      offset: number;
    }
  | { ok: false; error: string };

export type ListCondoNamesResponse =
  | { ok: true; condos: string[] }
  | { ok: false; error: string };

export type UpsertSaleTransactionRequest = Partial<{
  id: string;
  condo_name: string;
  property_type: string | null;
  unit_type: string;
  sqft: number;
  exact_level: number;
  exact_unit: string; // Stored but NEVER returned in responses
  level_low: number;
  level_high: number;
  purchase_date: string; // YYYY-MM-DD
  purchase_price: number;
  sale_date: string; // YYYY-MM-DD
  sale_price: number;
}>;

export type UpsertSaleTransactionResponse =
  | { ok: true; row: SaleTransaction }
  | { ok: false; error: string };

export type Listing = {
  id: string;
  external_id: string;
  title: string;
  address: string | null;
  price: string | null;
  psf: string | null;
  size: string | null;
  property_type: string | null;
  status: string;
  distance: string | null;
  link: string | null;
  listed_at: string | null; // YYYY-MM-DD
  photos: string[]; // can include image/video URLs
  created_at: string;
  updated_at: string;
};

export type ListListingsResponse =
  | {
      ok: true;
      rows: Listing[];
      total_count: number;
      limit: number;
      offset: number;
    }
  | { ok: false; error: string };

export type UpsertListingRequest = Partial<{
  id: string;
  external_id: string;
  title: string;
  address: string | null;
  price: string | null;
  psf: string | null;
  size: string | null;
  property_type: string | null;
  status: string;
  distance: string | null;
  link: string | null;
  listed_at: string | null; // YYYY-MM-DD
  photos: string[]; // image/video URLs
}>;

export type UpsertListingResponse =
  | { ok: true; row: Listing }
  | { ok: false; error: string };
