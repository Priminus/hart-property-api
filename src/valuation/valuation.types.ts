export type ValuationRequest = {
  condo_name: string;
  unit_number?: string; // e.g. "12-05" (floor-unit) - optional for landed properties
  sqft: number; // unit size in sqft
  email: string;
  name?: string;
};

export type ValuationResult = {
  condo_name: string;
  unit_number: string;
  unit_sqft: number;
  estimated_floor: number;
  floor_range: string; // e.g. "01 to 05"
  appreciation_rate: number; // Annual appreciation rate as decimal (e.g. 0.03 for 3%)
  estimated_psf_low: number;
  estimated_psf_mid: number;
  estimated_psf_high: number;
  estimated_price_low: number;
  estimated_price_mid: number;
  estimated_price_high: number;
  recent_transactions: {
    sale_price: number;
    sale_date: string;
    floor_range: string;
    sqft: number | null;
    psf: number | null;
    adjusted_psf: number | null; // PSF adjusted to today's value
  }[];
  data_period: string; // e.g. "Jan 2024 - Jan 2026"
  generated_at: string;
};

export type ValuationResponse =
  | { ok: true; message: string }
  | { ok: false; error: string };
