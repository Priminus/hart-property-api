-- Add unique constraint for URA deduplication at DB level
-- Key: LOWER(condo_name) + sale_price + sale_month (YYYYMM as integer)

-- Add column for sale_month as integer (YYYYMM format)
ALTER TABLE public.condo_sale_transactions
ADD COLUMN IF NOT EXISTS sale_month integer;

-- Populate sale_month for existing rows
UPDATE public.condo_sale_transactions
SET sale_month = EXTRACT(YEAR FROM sale_date)::int * 100 + EXTRACT(MONTH FROM sale_date)::int
WHERE sale_date IS NOT NULL AND sale_month IS NULL;

-- Delete duplicates keeping the row with the most data (has purchase_price)
-- or the first one if neither has purchase data
DELETE FROM public.condo_sale_transactions a
USING public.condo_sale_transactions b
WHERE a.id > b.id
  AND LOWER(a.condo_name) = LOWER(b.condo_name)
  AND a.sale_price = b.sale_price
  AND a.sale_month = b.sale_month
  AND (a.purchase_price IS NULL OR b.purchase_price IS NOT NULL);

-- Create unique index on the dedup key
-- This will prevent future duplicates at DB level
CREATE UNIQUE INDEX IF NOT EXISTS uniq_condo_sale_transactions_ura_dedup
  ON public.condo_sale_transactions (
    LOWER(condo_name),
    sale_price,
    sale_month
  )
  WHERE sale_month IS NOT NULL AND sale_price IS NOT NULL;

-- Create trigger to auto-populate sale_month on insert/update
CREATE OR REPLACE FUNCTION set_sale_month()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.sale_date IS NOT NULL THEN
    NEW.sale_month := EXTRACT(YEAR FROM NEW.sale_date)::int * 100 + EXTRACT(MONTH FROM NEW.sale_date)::int;
  ELSE
    NEW.sale_month := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_sale_month ON public.condo_sale_transactions;
CREATE TRIGGER trg_set_sale_month
BEFORE INSERT OR UPDATE ON public.condo_sale_transactions
FOR EACH ROW EXECUTE FUNCTION set_sale_month();
