ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS next_retry TIMESTAMP WITH TIME ZONE DEFAULT NULL;
ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS retries_remaining INT NOT NULL default 0;

ALTER TABLE transfer_tasks_child ADD COLUMN IF NOT EXISTS next_retry TIMESTAMP WITH TIME ZONE DEFAULT NULL;
DO $$
BEGIN
  -- Check if the column 'old_column_name' exists in the table 'your_table'
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema =  CURRENT_SCHEMA()
      AND table_name = 'transfer_tasks_child'
      AND column_name = 'retries'
  ) THEN
    -- If it exists, execute the RENAME COLUMN statement
    EXECUTE 'ALTER TABLE transfer_tasks_child RENAME COLUMN retries to retries_remaining';
  END IF;
END $$;