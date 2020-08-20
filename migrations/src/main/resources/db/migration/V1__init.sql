-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS transfer_tasks CASCADE;
CREATE table transfer_tasks
(
    id                    serial PRIMARY KEY,
    tenant_id             VARCHAR(265)             NOT NULL,
    created               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username              VARCHAR(256)             NOT NULL,
    uuid                  uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    source_system_id      VARCHAR                  NOT NULL,
    source_path           VARCHAR                  NOT NULL,
    destination_system_id VARCHAR                  NOT NULL,
    destination_path      VARCHAR                  NOT NULL,
    status                VARCHAR(128)             NOT NULL,
    bytes_transferred     BIGINT                   NOT NULL default 0,
    total_bytes           BIGINT                   NOT NULL default 0,
    retries               INT                      NOT NULL default 0,
    final_message         VARCHAR
);
CREATE index on transfer_tasks (tenant_id, username, uuid);

DROP TABLE IF EXISTS transfer_tasks_child CASCADE;
CREATE TABLE transfer_tasks_child
(
    id                    serial PRIMARY KEY,
    tenant_id             VARCHAR(265)             NOT NULL,
    created               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username              VARCHAR(256)             NOT NULL,
    uuid                  uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    source_system_id      VARCHAR                  NOT NULL,
    source_path           VARCHAR                  NOT NULL,
    destination_system_id VARCHAR                  NOT NULL,
    destination_path      VARCHAR                  NOT NULL,
    status                VARCHAR(128)             NOT NULL,
    bytes_transferred     BIGINT                   NOT NULL default 0,
    total_bytes           BIGINT                   NOT NULL default 0,
    retries               INT                      NOT NULL default 0,
    parent_task_id int REFERENCES transfer_tasks(id) ON DELETE CASCADE,
    final_message         VARCHAR
);

CREATE INDEX on transfer_tasks_child (tenant_id, username, parent_task_id);

DROP TABLE IF EXISTS transfer_task_child_events CASCADE;
CREATE TABLE transfer_task_child_events
(
    id            serial PRIMARY KEY,
    tenant_id     VARCHAR(265)             NOT NULL,
    child_task_id int REFERENCES transfer_tasks_child (id) ON DELETE CASCADE,
    created       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid          uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    event_name    VARCHAR(265)             NOT NULL,
    data          jsonb                    NOT NULL
);
CREATE INDEX on transfer_task_child_events (tenant_id, child_task_id);



DROP TABLE IF EXISTS shares CASCADE;
CREATE TABLE shares
(
    id                   serial PRIMARY KEY,
    tenant_id            VARCHAR(265)             NOT NULL,
    created              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username             VARCHAR(256)             NOT NULL,
    shared_with_username VARCHAR(256)             NOT NULL,
    system_id            VARCHAR                  NOT NULL,
    path                 VARCHAR                  NOT NULL,
    uuid                 uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    max_views            integer                  NOT NULL,
    views                integer                  NOT NULL
);
CREATE index on shares (tenant_id, username, uuid);
