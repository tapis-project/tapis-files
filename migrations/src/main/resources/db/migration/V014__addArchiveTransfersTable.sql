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
   archive_type             VARCHAR(32)                 NOT NULL,
   start_time               TIMESTAMP WITH TIME ZONE    DEFAULT NULL,
   end_time                 TIMESTAMP WITH TIME ZONE    DEFAULT NULL,
   next_retry               TIMESTAMP WITH TIME ZONE    DEFAULT NULL,
   retries_remaining        SMALLINT                    DEFAULT NULL,
   file_bytes_read          BIGINT                      NOT NULL,
   archive_bytes_read       BIGINT                      NOT NULL,
   error_message            TEXT                        DEFAULT NULL,
   src_shared_ctx           TEXT                        DEFAULT NULL,
   dst_shared_ctx           TEXT                        DEFAULT NULL,
   assigned_to              uuid                        DEFAULT NULL
 );
 CREATE INDEX ON archive_transfers (uuid);
 CREATE INDEX ON archive_transfers (id);

 CREATE TABLE IF NOT EXISTS archive_transfer_paths
 (
  archive_transfer_id       int REFERENCES archive_transfers(id)
                                                        ON DELETE CASCADE ON UPDATE CASCADE,
  path                      VARCHAR(4096)               NOT NULL
 );
 CREATE INDEX ON archive_transfer_paths (archive_transfer_id);


 create table if not exists archive_transfer_log
 (
  archive_transfer_id       int UNIQUE REFERENCES archive_transfers(id)
                                                        ON DELETE CASCADE ON UPDATE CASCADE,
  log_entries               jsonb
 );
 CREATE INDEX ON archive_transfer_log(archive_transfer_id);

 ALTER TABLE transfer_worker ADD COLUMN IF NOT EXISTS worker_config JSONB;