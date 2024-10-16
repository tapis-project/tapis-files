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
CREATE INDEX on transfer_tasks_child (status);

CREATE INDEX on transfer_tasks_parent (tenant_id);
CREATE INDEX on transfer_tasks_parent (username);
CREATE INDEX on transfer_tasks_parent (created);
CREATE INDEX on transfer_tasks_parent (assigned_to);
CREATE INDEX on transfer_tasks_parent (status);

UPDATE transfer_tasks_child SET status = 'FAILED' WHERE  created < now() - INTERVAL '7 days' AND status NOT IN ( 'COMPLETED', 'FAILED', 'FAILED_OPT', 'CANCELLED', 'PAUSED');
UPDATE transfer_tasks_parent SET status = 'FAILED' WHERE  created < now() - INTERVAL '7 days' AND status NOT IN ( 'COMPLETED', 'FAILED', 'FAILED_OPT', 'CANCELLED', 'PAUSED');
UPDATE transfer_tasks SET status = 'FAILED' WHERE  created < now() - INTERVAL '7 days' AND status NOT IN ( 'COMPLETED', 'FAILED', 'FAILED_OPT', 'CANCELLED', 'PAUSED');
