CREATE TABLE IF NOT EXISTS transfer_worker
(
  uuid           uuid                     NOT NULL DEFAULT uuid_generate_v4(),
  last_updated   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);