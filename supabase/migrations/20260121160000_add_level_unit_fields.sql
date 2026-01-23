-- Add level and unit fields to condo_sale_transactions
-- exact_unit is stored but NEVER exposed via API (guardrail in service layer)

alter table public.condo_sale_transactions
  add column if not exists exact_level integer,
  add column if not exists exact_unit integer,
  add column if not exists level_low integer,
  add column if not exists level_high integer;

-- Add a comment to remind developers about the guardrail
comment on column public.condo_sale_transactions.exact_unit is 
  'SENSITIVE: Never expose this field via API. Used for internal matching only.';
