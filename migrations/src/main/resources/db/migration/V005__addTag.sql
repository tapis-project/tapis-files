-- ------------------------------------------------------------------------------------------------------
-- Add column for transfer task tag to parent and child tables.
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS tag TEXT DEFAULT NULL;
ALTER TABLE transfer_tasks_child ADD COLUMN IF NOT EXISTS tag TEXT DEFAULT NULL;
