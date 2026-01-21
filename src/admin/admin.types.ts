export type CondoSaleTransaction = {
  id: string;
  condo_name: string;
  unit_type: string;
  sqft: number | null;
  purchase_date: string | null; // YYYY-MM-DD
  purchase_price: number | null;
  sale_date: string | null; // YYYY-MM-DD
  sale_price: number | null;
  profit: number | null;
  annualised_pct: number | null;
  created_at: string;
};

export type ListCondoSaleTransactionsResponse =
  | {
      ok: true;
      rows: CondoSaleTransaction[];
      total_count: number;
      limit: number;
      offset: number;
    }
  | { ok: false; error: string };

export type ListCondoNamesResponse =
  | { ok: true; condos: string[] }
  | { ok: false; error: string };

export type UpsertCondoSaleTransactionRequest = Partial<{
  id: string;
  condo_name: string;
  unit_type: string;
  sqft: number;
  purchase_date: string; // YYYY-MM-DD
  purchase_price: number;
  sale_date: string; // YYYY-MM-DD
  sale_price: number;
}>;

export type UpsertCondoSaleTransactionResponse =
  | { ok: true; row: CondoSaleTransaction }
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

