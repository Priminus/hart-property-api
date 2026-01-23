-- Replace the old URA-only constraint with one that includes level and unit.
-- This allows upserting precise unit data from screenshots without conflicting with other units at the same price.

ALTER TABLE public.condo_sale_transactions
DROP CONSTRAINT IF EXISTS uniq_condo_sale_ura_dedup;

-- New constraint for upsert logic
ALTER TABLE public.condo_sale_transactions
ADD CONSTRAINT uniq_condo_sale_manual_upsert
UNIQUE (condo_name_lower, sale_price, sale_month, exact_level, exact_unit);
