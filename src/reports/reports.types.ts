import type { LeadAttribution, LeadContext } from '../leads/leads.types';

export type SendMarketOutlook2026Request = {
  email: string;
  utm?: LeadAttribution;
  context?: LeadContext;
};

export type CondoSaleProfitabilityResponse =
  | {
      ok: true;
      condo_name: string;
      transaction_count: number;
      median_profitability_pct: number;
      median_annualised_pct: number;
    }
  | { ok: false; error: string };

export type CondoSaleProfitabilityByYearResponse =
  | {
      ok: true;
      condos: string[];
      years: number[];
      series: Array<{
        condo_name: string;
        points: Array<{
          year: number;
          transaction_count: number;
          median_profitability_pct: number | null;
        }>;
      }>;
    }
  | { ok: false; error: string };

export type CondoSaleProfitabilityRowsResponse =
  | {
      ok: true;
      condos: string[];
      rows: Array<{
        condo_name: string;
        unit_type: string | null;
        sale_month: string; // YYYY-MM
        profitability_pct: number;
      }>;
    }
  | { ok: false; error: string };


