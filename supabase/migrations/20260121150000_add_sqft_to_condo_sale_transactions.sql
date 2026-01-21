-- Add sqft column to condo_sale_transactions
alter table public.condo_sale_transactions
  add column if not exists sqft integer;

-- Update existing Liv@MB transactions with sqft values
-- Matching on purchase_price and purchase_date to identify unique transactions

-- 2BR, 753 sqft - purchase 24 May 2022 $1,957,000
update public.condo_sale_transactions
set sqft = 753
where condo_name = 'Liv @ MB'
  and purchase_price = 1957000
  and purchase_date = '2022-05-24';

-- 3BR, 1281 sqft - purchase 21 May 2022 $2,792,000
update public.condo_sale_transactions
set sqft = 1281
where condo_name = 'Liv @ MB'
  and purchase_price = 2792000
  and purchase_date = '2022-05-21';

-- 2BR, 797 sqft - purchase 21 May 2022 $1,936,000
update public.condo_sale_transactions
set sqft = 797
where condo_name = 'Liv @ MB'
  and purchase_price = 1936000
  and purchase_date = '2022-05-21';

-- 3BR, 1270 sqft - purchase 21 May 2022 $3,048,000
update public.condo_sale_transactions
set sqft = 1270
where condo_name = 'Liv @ MB'
  and purchase_price = 3048000
  and purchase_date = '2022-05-21';

-- 2BR, 797 sqft - purchase 21 May 2022 $1,976,000
update public.condo_sale_transactions
set sqft = 797
where condo_name = 'Liv @ MB'
  and purchase_price = 1976000
  and purchase_date = '2022-05-21';

-- 2BR, 861 sqft - purchase 21 May 2022 $1,898,000
update public.condo_sale_transactions
set sqft = 861
where condo_name = 'Liv @ MB'
  and purchase_price = 1898000
  and purchase_date = '2022-05-21';

-- 2BR, 678 sqft - purchase 21 May 2022 $1,516,000
update public.condo_sale_transactions
set sqft = 678
where condo_name = 'Liv @ MB'
  and purchase_price = 1516000
  and purchase_date = '2022-05-21';

-- 2BR, 829 sqft - purchase 21 Jun 2022 $1,828,000
update public.condo_sale_transactions
set sqft = 829
where condo_name = 'Liv @ MB'
  and purchase_price = 1828000
  and purchase_date = '2022-06-21';

-- 3BR, 1270 sqft - purchase 21 May 2022 $3,215,000
update public.condo_sale_transactions
set sqft = 1270
where condo_name = 'Liv @ MB'
  and purchase_price = 3215000
  and purchase_date = '2022-05-21';

-- 2BR, 678 sqft - purchase 21 May 2022 $1,508,000
update public.condo_sale_transactions
set sqft = 678
where condo_name = 'Liv @ MB'
  and purchase_price = 1508000
  and purchase_date = '2022-05-21';

-- 2BR, 861 sqft - purchase 21 May 2022 $2,040,000
update public.condo_sale_transactions
set sqft = 861
where condo_name = 'Liv @ MB'
  and purchase_price = 2040000
  and purchase_date = '2022-05-21';

-- 2BR, 861 sqft - purchase 21 May 2022 $1,972,000
update public.condo_sale_transactions
set sqft = 861
where condo_name = 'Liv @ MB'
  and purchase_price = 1972000
  and purchase_date = '2022-05-21';
