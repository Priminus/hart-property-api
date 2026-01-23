-- Allow unit_type to be null (URA API doesn't provide bedroom count)
ALTER TABLE public.condo_sale_transactions
  ALTER COLUMN unit_type DROP NOT NULL;
