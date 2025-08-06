 CREATE TABLE IF NOT EXISTS archive_transfers
 (
   id                       serial                      PRIMARY KEY,
   uuid                     uuid                        NOT NULL DEFAULT uuid_generate_v4(),
   username                 VARCHAR(256)                NOT NULL,
   tenant_id                VARCHAR(265)                NOT NULL,
   created                  TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT CURRENT_TIMESTAMP,
   status                   VARCHAR(128)                NOT NULL,
   source_base_url          VARCHAR(4096)               NOT NULL,
   destination_base_url     VARCHAR(4096)               NOT NULL,
   start_time               TIMESTAMP WITH TIME ZONE    DEFAULT NULL,
   end_time                 TIMESTAMP WITH TIME ZONE    DEFAULT NULL,
   bytes_transferred        BIGINT                      NOT NULL,
   error_message            TEXT                        DEFAULT NULL,
   src_shared_ctx           TEXT                        DEFAULT NULL,
   dst_shared_ctx           TEXT                        DEFAULT NULL,
   assigned_to              uuid                        DEFAULT NULL
 );
 CREATE INDEX ON transfer_tasks (uuid);

 CREATE TABLE IF NOT EXISTS archive_transfer_paths
 (
  archive_transfer_id       int REFERENCES archive_transfers(id)
                                                        ON DELETE CASCADE ON UPDATE CASCADE,
  path                      VARCHAR(4096)               NOT NULL
 );
 CREATE INDEX ON transfer_tasks (id);
