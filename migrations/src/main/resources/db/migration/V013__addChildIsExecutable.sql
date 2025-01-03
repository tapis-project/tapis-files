-- ------------------------------------------------------------------------------------------------------
-- Add column is_executable to child tasks.  This will let us know if we need to set the executable bit on transfers.
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks_child ADD COLUMN IF NOT EXISTS is_executable TEXT DEFAULT false;
