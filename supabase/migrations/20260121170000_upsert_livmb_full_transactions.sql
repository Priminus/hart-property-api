-- Upsert Liv @ MB transactions with full purchase/sale data
-- Uses the existing unique index on (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price)

-- Helper function to compute annualized return
CREATE OR REPLACE FUNCTION compute_annualised_pct(
  purchase_price numeric,
  sale_price numeric,
  purchase_date date,
  sale_date date
) RETURNS numeric AS $$
DECLARE
  days_held numeric;
  cagr numeric;
BEGIN
  days_held := sale_date - purchase_date;
  IF days_held <= 0 OR purchase_price <= 0 THEN
    RETURN NULL;
  END IF;
  cagr := (POWER(sale_price / purchase_price, 365.0 / days_held) - 1) * 100;
  RETURN ROUND(cagr, 2);
END;
$$ LANGUAGE plpgsql;

-- Upsert each transaction using ON CONFLICT with the full natural key
-- If row exists, update the extra fields (sqft, exact_level, exact_unit, profit, annualised_pct)

-- 1. 2BR, Level 14, Unit 12, 753 sqft - Purchase: 24 May 2022 $1,957,000 - Sale: 23 Dec 2025 $2,013,888
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 753, 14, '12',
  '2022-05-24', 1957000, '2025-12-23', 2013888,
  2013888 - 1957000,
  compute_annualised_pct(1957000, 2013888, '2022-05-24', '2025-12-23')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 2. 3BR, Level 2, Unit 14, 1281 sqft - Purchase: 21 May 2022 $2,792,000 - Sale: 16 Dec 2025 $3,290,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '3BR', 1281, 2, '14',
  '2022-05-21', 2792000, '2025-12-16', 3290000,
  3290000 - 2792000,
  compute_annualised_pct(2792000, 3290000, '2022-05-21', '2025-12-16')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 3. 2BR, Level 3, Unit 05, 797 sqft - Purchase: 21 May 2022 $1,936,000 - Sale: 05 Nov 2025 $2,220,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 797, 3, '05',
  '2022-05-21', 1936000, '2025-11-05', 2220000,
  2220000 - 1936000,
  compute_annualised_pct(1936000, 2220000, '2022-05-21', '2025-11-05')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 4. 3BR, Level 10, Unit 14, 1270 sqft - Purchase: 21 May 2022 $3,048,000 - Sale: 21 Oct 2025 $3,328,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '3BR', 1270, 10, '14',
  '2022-05-21', 3048000, '2025-10-21', 3328000,
  3328000 - 3048000,
  compute_annualised_pct(3048000, 3328000, '2022-05-21', '2025-10-21')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 5. 2BR, Level 7, Unit 05, 797 sqft - Purchase: 21 May 2022 $1,976,000 - Sale: 21 Oct 2025 $2,268,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 797, 7, '05',
  '2022-05-21', 1976000, '2025-10-21', 2268000,
  2268000 - 1976000,
  compute_annualised_pct(1976000, 2268000, '2022-05-21', '2025-10-21')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 6. 2BR, Level 2, Unit 09, 861 sqft - Purchase: 21 May 2022 $1,898,000 - Sale: 24 Sep 2025 $2,185,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 861, 2, '09',
  '2022-05-21', 1898000, '2025-09-24', 2185000,
  2185000 - 1898000,
  compute_annualised_pct(1898000, 2185000, '2022-05-21', '2025-09-24')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 7. 2BR, Level 10, Unit 07, 678 sqft - Purchase: 21 May 2022 $1,516,000 - Sale: 19 Sep 2025 $1,930,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 678, 10, '07',
  '2022-05-21', 1516000, '2025-09-19', 1930000,
  1930000 - 1516000,
  compute_annualised_pct(1516000, 1930000, '2022-05-21', '2025-09-19')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 8. 2BR, Level 3, Unit 08, 829 sqft - Purchase: 21 Jun 2022 $1,828,000 - Sale: 07 Aug 2025 $2,118,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 829, 3, '08',
  '2022-06-21', 1828000, '2025-08-07', 2118000,
  2118000 - 1828000,
  compute_annualised_pct(1828000, 2118000, '2022-06-21', '2025-08-07')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 9. 3BR, Level 12, Unit 02, 1270 sqft - Purchase: 21 May 2022 $3,215,000 - Sale: 06 Aug 2025 $3,500,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '3BR', 1270, 12, '02',
  '2022-05-21', 3215000, '2025-08-06', 3500000,
  3500000 - 3215000,
  compute_annualised_pct(3215000, 3500000, '2022-05-21', '2025-08-06')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 10. 2BR, Level 9, Unit 07, 678 sqft - Purchase: 21 May 2022 $1,508,000 - Sale: 31 Jul 2025 $1,880,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 678, 9, '07',
  '2022-05-21', 1508000, '2025-07-31', 1880000,
  1880000 - 1508000,
  compute_annualised_pct(1508000, 1880000, '2022-05-21', '2025-07-31')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 11. 2BR, Level 12, Unit 09, 861 sqft - Purchase: 21 May 2022 $2,040,000 - Sale: 17 Jul 2025 $2,330,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 861, 12, '09',
  '2022-05-21', 2040000, '2025-07-17', 2330000,
  2330000 - 2040000,
  compute_annualised_pct(2040000, 2330000, '2022-05-21', '2025-07-17')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- 12. 2BR, Level 8, Unit 09, 861 sqft - Purchase: 21 May 2022 $1,972,000 - Sale: 16 Jul 2025 $2,280,000
INSERT INTO public.condo_sale_transactions (
  condo_name, unit_type, sqft, exact_level, exact_unit,
  purchase_date, purchase_price, sale_date, sale_price, profit, annualised_pct
) VALUES (
  'Liv @ MB', '2BR', 861, 8, '09',
  '2022-05-21', 1972000, '2025-07-16', 2280000,
  2280000 - 1972000,
  compute_annualised_pct(1972000, 2280000, '2022-05-21', '2025-07-16')
)
ON CONFLICT (condo_name, unit_type, purchase_date, purchase_price, sale_date, sale_price) 
DO UPDATE SET
  sqft = EXCLUDED.sqft,
  exact_level = EXCLUDED.exact_level,
  exact_unit = EXCLUDED.exact_unit,
  profit = EXCLUDED.profit,
  annualised_pct = EXCLUDED.annualised_pct;

-- Drop the helper function
DROP FUNCTION IF EXISTS compute_annualised_pct(numeric, numeric, date, date);
