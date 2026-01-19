create unique index if not exists uniq_condo_sale_transactions_natural_key
  on public.condo_sale_transactions (
    condo_name,
    unit_type,
    purchase_date,
    purchase_price,
    sale_date,
    sale_price
  );

