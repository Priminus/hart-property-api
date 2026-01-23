-- Allow multiple transactions with same price/month if they have different exact units
-- This prevents the "53 txns vs 77 txns" issue where different units sold at same launch price

ALTER TABLE public.condo_sale_transactions
DROP CONSTRAINT IF EXISTS uniq_condo_sale_ura_dedup;

DROP INDEX IF EXISTS public.uniq_condo_sale_ura_dedup;
DROP INDEX IF EXISTS public.uniq_condo_sale_manual;

-- 1. URA-style rows (no exact unit info) remain deduped by price/month
CREATE UNIQUE INDEX uniq_condo_sale_ura_dedup
  ON public.condo_sale_transactions (condo_name_lower, sale_price, sale_month)
  WHERE exact_level IS NULL AND exact_unit IS NULL;

-- 2. Manual/Screenshot rows (with exact unit) are unique by unit + price + month
CREATE UNIQUE INDEX uniq_condo_sale_manual
  ON public.condo_sale_transactions (
    condo_name_lower,
    sale_price,
    sale_month,
    exact_level,
    exact_unit
  )
  WHERE exact_level IS NOT NULL AND exact_unit IS NOT NULL;
