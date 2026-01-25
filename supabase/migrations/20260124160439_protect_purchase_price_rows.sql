-- GUARDRAIL: Never delete rows that have purchase_price data.
-- These contain valuable manually-entered data that must be preserved.

CREATE OR REPLACE FUNCTION merge_ura_transactions_batch(
  transactions jsonb
) RETURNS jsonb AS $$
DECLARE
  txn jsonb;
  merged_count integer := 0;
  inserted_count integer := 0;
  v_sale_month integer;
  v_condo_name_lower text;
  v_sale_date date;
  v_sale_price numeric;
  v_updated_exact boolean;
  v_updated_generic boolean;
BEGIN
  FOR txn IN SELECT * FROM jsonb_array_elements(transactions)
  LOOP
    v_sale_date := (txn->>'sale_date')::date;
    v_sale_month := EXTRACT(YEAR FROM v_sale_date)::int * 100 + EXTRACT(MONTH FROM v_sale_date)::int;
    v_condo_name_lower := LOWER(txn->>'condo_name');
    v_sale_price := (txn->>'sale_price')::numeric;
    
    -- First check: Does an OCR row (with exact_level) exist for this transaction?
    -- If so, update it and DON'T create/keep a URA row
    UPDATE sale_transactions
    SET 
      property_type = COALESCE(property_type, txn->>'property_type'),
      type_of_sale = COALESCE(type_of_sale, txn->>'type_of_sale'),
      level_low = COALESCE(level_low, (txn->>'level_low')::numeric),
      level_high = COALESCE(level_high, (txn->>'level_high')::numeric)
    WHERE condo_name_lower = v_condo_name_lower
      AND sale_price = v_sale_price
      AND sale_month = v_sale_month
      AND exact_level IS NOT NULL;
    
    v_updated_exact := FOUND;
    
    IF v_updated_exact THEN
      -- OCR row exists - delete any duplicate URA row (exact_level IS NULL)
      -- GUARDRAIL: NEVER delete rows that have purchase_price!
      DELETE FROM sale_transactions
      WHERE condo_name_lower = v_condo_name_lower
        AND sale_price = v_sale_price
        AND sale_month = v_sale_month
        AND exact_level IS NULL
        AND purchase_price IS NULL;  -- PROTECT rows with purchase_price
      
      merged_count := merged_count + 1;
    ELSE
      -- No OCR row exists - check if a row with purchase_price exists
      -- If so, update it; otherwise update/insert URA row
      UPDATE sale_transactions
      SET
        sqft = COALESCE(sqft, (txn->>'sqft')::numeric),
        level_low = COALESCE(level_low, (txn->>'level_low')::numeric),
        level_high = COALESCE(level_high, (txn->>'level_high')::numeric),
        type_of_sale = COALESCE(type_of_sale, txn->>'type_of_sale'),
        property_type = COALESCE(property_type, txn->>'property_type'),
        sale_date = COALESCE(sale_date, v_sale_date)
      WHERE condo_name_lower = v_condo_name_lower
        AND sale_price = v_sale_price
        AND sale_month = v_sale_month
        AND exact_level IS NULL;
      
      v_updated_generic := FOUND;
      
      IF v_updated_generic THEN
        merged_count := merged_count + 1;
      ELSE
        -- No row exists at all - insert new URA row
        INSERT INTO sale_transactions (
          condo_name, sale_price, sale_date, sqft, level_low, level_high, type_of_sale, property_type
        ) VALUES (
          txn->>'condo_name',
          v_sale_price,
          v_sale_date,
          (txn->>'sqft')::numeric,
          (txn->>'level_low')::numeric,
          (txn->>'level_high')::numeric,
          txn->>'type_of_sale',
          txn->>'property_type'
        )
        ON CONFLICT (condo_name_lower, sale_price, sale_month, exact_level, exact_unit) 
        DO NOTHING;
        
        IF FOUND THEN
          inserted_count := inserted_count + 1;
        END IF;
      END IF;
    END IF;
  END LOOP;
  
  RETURN jsonb_build_object('merged', merged_count, 'inserted', inserted_count);
END;
$$ LANGUAGE plpgsql;

-- Also update the single-row function with the same guardrail
CREATE OR REPLACE FUNCTION merge_ura_transaction(
  p_condo_name text,
  p_sale_price numeric,
  p_sale_date date,
  p_sqft numeric,
  p_level_low numeric,
  p_level_high numeric,
  p_type_of_sale text,
  p_property_type text
) RETURNS void AS $$
DECLARE
  v_sale_month integer;
  v_condo_name_lower text;
  v_updated_exact boolean;
  v_updated_generic boolean;
BEGIN
  v_sale_month := EXTRACT(YEAR FROM p_sale_date)::int * 100 + EXTRACT(MONTH FROM p_sale_date)::int;
  v_condo_name_lower := LOWER(p_condo_name);
  
  -- First: Try to update OCR row (with exact_level)
  UPDATE sale_transactions
  SET 
    property_type = COALESCE(property_type, p_property_type),
    type_of_sale = COALESCE(type_of_sale, p_type_of_sale),
    level_low = COALESCE(level_low, p_level_low),
    level_high = COALESCE(level_high, p_level_high)
  WHERE condo_name_lower = v_condo_name_lower
    AND sale_price = p_sale_price
    AND sale_month = v_sale_month
    AND exact_level IS NOT NULL;
  
  v_updated_exact := FOUND;
  
  IF v_updated_exact THEN
    -- Delete duplicate URA row if exists
    -- GUARDRAIL: NEVER delete rows that have purchase_price!
    DELETE FROM sale_transactions
    WHERE condo_name_lower = v_condo_name_lower
      AND sale_price = p_sale_price
      AND sale_month = v_sale_month
      AND exact_level IS NULL
      AND purchase_price IS NULL;  -- PROTECT rows with purchase_price
  ELSE
    -- No OCR row - update or insert URA row
    UPDATE sale_transactions
    SET
      sqft = COALESCE(sqft, p_sqft),
      level_low = COALESCE(level_low, p_level_low),
      level_high = COALESCE(level_high, p_level_high),
      type_of_sale = COALESCE(type_of_sale, p_type_of_sale),
      property_type = COALESCE(property_type, p_property_type),
      sale_date = COALESCE(sale_date, p_sale_date)
    WHERE condo_name_lower = v_condo_name_lower
      AND sale_price = p_sale_price
      AND sale_month = v_sale_month
      AND exact_level IS NULL;
    
    v_updated_generic := FOUND;
    
    IF NOT v_updated_generic THEN
      INSERT INTO sale_transactions (
        condo_name, sale_price, sale_date, sqft, level_low, level_high, type_of_sale, property_type
      ) VALUES (
        p_condo_name, p_sale_price, p_sale_date, p_sqft, p_level_low, p_level_high, p_type_of_sale, p_property_type
      )
      ON CONFLICT (condo_name_lower, sale_price, sale_month, exact_level, exact_unit) 
      DO NOTHING;
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql;
