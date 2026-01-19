create table if not exists public.condo_sale_transactions (
  id uuid primary key default gen_random_uuid(),
  condo_name text not null,
  unit_type text not null,
  purchase_date date not null,
  purchase_price numeric not null,
  sale_date date not null,
  sale_price numeric not null,
  profit numeric not null,
  annualised_pct numeric not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_condo_sale_transactions_condo_name
  on public.condo_sale_transactions (condo_name);

create index if not exists idx_condo_sale_transactions_sale_date
  on public.condo_sale_transactions (sale_date);

insert into public.condo_sale_transactions (
  condo_name,
  unit_type,
  purchase_date,
  purchase_price,
  sale_date,
  sale_price,
  profit,
  annualised_pct
)
values
  ('Liv @ MB', '2BR', '2022-05-24', 1957000, '2025-12-23', 2013888, 56888, 0.8),
  ('Liv @ MB', '3BR', '2022-05-21', 2792000, '2025-12-16', 3290000, 498000, 4.7),
  ('Liv @ MB', '2BR', '2022-05-21', 1936000, '2025-11-05', 2220000, 284000, 4.03),
  ('Liv @ MB', '3BR', '2022-05-21', 3048000, '2025-10-21', 3328000, 280000, 2.6)
on conflict do nothing;

