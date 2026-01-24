-- Rename the table
ALTER TABLE public.condo_sale_transactions RENAME TO sale_transactions;

-- Add property_type column
ALTER TABLE public.sale_transactions ADD COLUMN IF NOT EXISTS property_type text;

-- Rename indexes/constraints to be consistent
ALTER TABLE public.sale_transactions RENAME CONSTRAINT uniq_condo_sale_manual_upsert TO uniq_sale_manual_upsert;

-- Re-apply the merge functions with the new table name
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
BEGIN
  v_sale_month := EXTRACT(YEAR FROM p_sale_date)::int * 100 + EXTRACT(MONTH FROM p_sale_date)::int;
  v_condo_name_lower := LOWER(p_condo_name);
  
  -- Try to update existing generic row first (merge)
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
  
  -- If no row was updated, insert new one
  IF NOT FOUND THEN
    INSERT INTO sale_transactions (
      condo_name, sale_price, sale_date, sqft, level_low, level_high, type_of_sale, property_type
    ) VALUES (
      p_condo_name, p_sale_price, p_sale_date, p_sqft, p_level_low, p_level_high, p_type_of_sale, p_property_type
    )
    ON CONFLICT (condo_name_lower, sale_price, sale_month, exact_level, exact_unit) 
    DO NOTHING;
  END IF;
END;
$$ LANGUAGE plpgsql;

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
  v_updated boolean;
BEGIN
  FOR txn IN SELECT * FROM jsonb_array_elements(transactions)
  LOOP
    v_sale_date := (txn->>'sale_date')::date;
    v_sale_month := EXTRACT(YEAR FROM v_sale_date)::int * 100 + EXTRACT(MONTH FROM v_sale_date)::int;
    v_condo_name_lower := LOWER(txn->>'condo_name');
    
    -- Try to update existing generic row
    UPDATE sale_transactions
    SET
      sqft = COALESCE(sqft, (txn->>'sqft')::numeric),
      level_low = COALESCE(level_low, (txn->>'level_low')::numeric),
      level_high = COALESCE(level_high, (txn->>'level_high')::numeric),
      type_of_sale = COALESCE(type_of_sale, txn->>'type_of_sale'),
      property_type = COALESCE(property_type, txn->>'property_type'),
      sale_date = COALESCE(sale_date, v_sale_date)
    WHERE condo_name_lower = v_condo_name_lower
      AND sale_price = (txn->>'sale_price')::numeric
      AND sale_month = v_sale_month
      AND exact_level IS NULL;
    
    v_updated := FOUND;
    
    IF v_updated THEN
      merged_count := merged_count + 1;
    ELSE
      -- Insert new row
      INSERT INTO sale_transactions (
        condo_name, sale_price, sale_date, sqft, level_low, level_high, type_of_sale, property_type
      ) VALUES (
        txn->>'condo_name',
        (txn->>'sale_price')::numeric,
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
  END LOOP;
  
  RETURN jsonb_build_object('merged', merged_count, 'inserted', inserted_count);
END;
$$ LANGUAGE plpgsql;
