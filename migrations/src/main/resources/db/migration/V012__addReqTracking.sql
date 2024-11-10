-- ------------------------------------------------------------------------------------------------------
-- Add column for auditing of tracking id that comes in with a transfer request
-- Must be in top, parent and child tables
-- TODO or does it just need to be in the top level task? Maybe all 3 for convenience, performance and clarity?
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks ADD COLUMN IF NOT EXISTS req_tracking_id TEXT DEFAULT NULL;
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS req_tracking_id TEXT DEFAULT NULL;
ALTER TABLE transfer_tasks_child ADD COLUMN IF NOT EXISTS req_tracking_id TEXT DEFAULT NULL;
