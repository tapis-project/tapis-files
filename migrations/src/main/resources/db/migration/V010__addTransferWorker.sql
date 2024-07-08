CREATE TABLE IF NOT EXISTS transfer_worker
(
  uuid           uuid                     NOT NULL DEFAULT uuid_generate_v4(),
  last_updated   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE transfer_tasks_parent ADD COLUMN IF NOT EXISTS assigned_to uuid;
ALTER TABLE transfer_tasks_child  ADD COLUMN IF NOT EXISTS assigned_to uuid;

CREATE INDEX on transfer_tasks_child (tenant_id);
CREATE INDEX on transfer_tasks_child (username);
CREATE INDEX on transfer_tasks_child (created);
CREATE INDEX on transfer_tasks_child (assigned_to);
