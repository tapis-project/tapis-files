-- ------------------------------------------------------------------------------------------------------
-- Add column for auditing of tracking id that comes in with a transfer request
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks ADD COLUMN IF NOT EXISTS parent_tracking_id TEXT DEFAULT NULL;
