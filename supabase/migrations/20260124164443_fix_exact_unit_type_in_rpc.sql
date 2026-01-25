-- Fix exact_unit type to be integer in the RPC function

DROP FUNCTION IF EXISTS upsert_ocr_transaction(text, text, date, numeric, integer, numeric, text, integer, text, text, text);

CREATE OR REPLACE FUNCTION upsert_ocr_transaction(
  p_condo_name text,
  p_condo_name_lower text,
  p_sale_date date,
  p_sale_price numeric,
  p_sale_month integer,
  p_sqft numeric,
  p_unit_type text,
  p_exact_level integer,
  p_exact_unit integer,  -- Changed from text to integer
  p_type_of_sale text,
  p_property_type text
) RETURNS void AS $$
BEGIN
  INSERT INTO sale_transactions (
    condo_name, condo_name_lower, sale_date, sale_price, sale_month,
    sqft, unit_type, exact_level, exact_unit, type_of_sale, property_type
  ) VALUES (
    p_condo_name, p_condo_name_lower, p_sale_date, p_sale_price, p_sale_month,
    p_sqft, p_unit_type, p_exact_level, p_exact_unit, p_type_of_sale, p_property_type
  )
  ON CONFLICT (condo_name_lower, sale_price, sale_month, exact_level, exact_unit)
  DO UPDATE SET
    condo_name = COALESCE(EXCLUDED.condo_name, sale_transactions.condo_name),
    sale_date = COALESCE(EXCLUDED.sale_date, sale_transactions.sale_date),
    sqft = COALESCE(EXCLUDED.sqft, sale_transactions.sqft),
    unit_type = COALESCE(EXCLUDED.unit_type, sale_transactions.unit_type),
    type_of_sale = COALESCE(EXCLUDED.type_of_sale, sale_transactions.type_of_sale),
    property_type = COALESCE(EXCLUDED.property_type, sale_transactions.property_type);
    -- NEVER touch: purchase_price, purchase_date, profit, annualised_pct, level_low, level_high
END;
$$ LANGUAGE plpgsql;
