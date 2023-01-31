-- ------------------------------------------------------------------------------------------------------
-- Convert and rename columns src_app_ctx, dest_shared_ctx in table transfer_tasks_parent
-- Existing boolean values are no longer relevant so drop old columns and add new ones.
-- ------------------------------------------------------------------------------------------------------
ALTER TABLE transfer_tasks_parent DROP COLUMN IF EXISTS src_shared_app_ctx;
ALTER TABLE transfer_tasks_parent DROP COLUMN IF EXISTS dest_shared_app_ctx;
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS src_shared_ctx TEXT DEFAULT NULL;
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS dst_shared_ctx TEXT DEFAULT NULL;
