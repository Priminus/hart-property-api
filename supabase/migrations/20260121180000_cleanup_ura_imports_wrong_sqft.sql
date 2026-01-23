-- Clean up URA-imported transactions with incorrect sqft values
-- URA data was imported with SQM values stored as SQFT (without conversion)
-- Delete rows that came from URA (identified by: no purchase_price and unit_type guessed from area)

DELETE FROM public.condo_sale_transactions
WHERE purchase_price IS NULL
  AND unit_type IN ('Unknown', 'Studio', '1BR', '2BR', '3BR', '4BR', '5BR+');
