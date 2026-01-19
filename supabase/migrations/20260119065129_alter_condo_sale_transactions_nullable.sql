alter table public.condo_sale_transactions
  alter column purchase_date drop not null,
  alter column purchase_price drop not null,
  alter column sale_date drop not null,
  alter column sale_price drop not null,
  alter column profit drop not null,
  alter column annualised_pct drop not null;

