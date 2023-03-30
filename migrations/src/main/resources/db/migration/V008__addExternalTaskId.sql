-- ------------------------------------------------------------------------------------------------------
-- Add column for child transfer task external txfr id.
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks_child ADD COLUMN IF NOT EXISTS external_task_id TEXT NOT NULL DEFAULT "";
