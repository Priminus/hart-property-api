-- 1. Clear out URA-imported data (uppercase condo names + null unit_type)
DELETE FROM public.condo_sale_transactions
WHERE unit_type IS NULL
  AND condo_name = UPPER(condo_name);

-- 2. Add type_of_sale column
-- URA API returns: 1 = "New Sale", 2 = "Sub Sale", 3 = "Resale"
ALTER TABLE public.condo_sale_transactions
ADD COLUMN IF NOT EXISTS type_of_sale text;

-- 3. Add comment for documentation
COMMENT ON COLUMN public.condo_sale_transactions.type_of_sale IS 'From URA: New Sale, Sub Sale, or Resale';
