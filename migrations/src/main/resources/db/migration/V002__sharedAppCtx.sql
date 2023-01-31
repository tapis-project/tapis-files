-- ------------------------------------------------------------------------------------------------------
-- Add columns src_shared_app_ctx, dest_shared_app_ctx to table transfer_tasks_parent
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS src_shared_app_ctx BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS dest_shared_app_ctx BOOLEAN NOT NULL DEFAULT FALSE;
