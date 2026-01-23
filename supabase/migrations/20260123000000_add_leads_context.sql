-- Add context column to leads table for storing valuation tool data
ALTER TABLE leads ADD COLUMN IF NOT EXISTS context jsonb;

-- Add comment
COMMENT ON COLUMN leads.context IS 'JSON context data (e.g., valuation request details)';
