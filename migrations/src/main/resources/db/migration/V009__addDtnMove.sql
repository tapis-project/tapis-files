-- ------------------------------------------------------------------------------------------------------
-- Add column for child transfer task external txfr id.
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS transfer_type TEXT DEFAULT null;

