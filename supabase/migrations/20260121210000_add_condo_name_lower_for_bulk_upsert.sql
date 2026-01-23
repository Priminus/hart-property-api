-- Add condo_name_lower column for bulk upsert compatibility
-- Supabase upsert requires column names, not expressions like LOWER()

-- Drop old expression-based index
DROP INDEX IF EXISTS uniq_condo_sale_transactions_ura_dedup;

-- Add condo_name_lower column
ALTER TABLE public.condo_sale_transactions
ADD COLUMN IF NOT EXISTS condo_name_lower text;

-- Populate existing rows
UPDATE public.condo_sale_transactions
SET condo_name_lower = LOWER(condo_name)
WHERE condo_name_lower IS NULL;

-- Update trigger to also set condo_name_lower
CREATE OR REPLACE FUNCTION set_sale_month_and_lower_name()
RETURNS TRIGGER AS $$
BEGIN
  -- Set sale_month from sale_date
  IF NEW.sale_date IS NOT NULL THEN
    NEW.sale_month := EXTRACT(YEAR FROM NEW.sale_date)::int * 100 + EXTRACT(MONTH FROM NEW.sale_date)::int;
  ELSE
    NEW.sale_month := NULL;
  END IF;
  
  -- Set condo_name_lower
  IF NEW.condo_name IS NOT NULL THEN
    NEW.condo_name_lower := LOWER(NEW.condo_name);
  ELSE
    NEW.condo_name_lower := NULL;
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_sale_month ON public.condo_sale_transactions;
DROP TRIGGER IF EXISTS trg_set_sale_month_and_lower ON public.condo_sale_transactions;
CREATE TRIGGER trg_set_sale_month_and_lower
BEFORE INSERT OR UPDATE ON public.condo_sale_transactions
FOR EACH ROW EXECUTE FUNCTION set_sale_month_and_lower_name();

-- Create unique constraint (not just index) for upsert to work
ALTER TABLE public.condo_sale_transactions
ADD CONSTRAINT uniq_condo_sale_ura_dedup 
UNIQUE (condo_name_lower, sale_price, sale_month);
