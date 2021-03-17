--CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE IF NOT EXISTS transfer_tasks
(
    id                    serial PRIMARY KEY,
    tenant_id             VARCHAR(265)             NOT NULL,
    created               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username              VARCHAR(256)             NOT NULL,
    uuid                  uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    status                VARCHAR(128)             NOT NULL,
    tag                   VARCHAR(265),
    start_time               TIMESTAMP WITH TIME ZONE,
    end_time               TIMESTAMP WITH TIME ZONE,
    error_message           TEXT
);
CREATE INDEX ON transfer_tasks (uuid);


CREATE TABLE IF NOT EXISTS transfer_tasks_parent
(
    id                    serial PRIMARY KEY,
    tenant_id             VARCHAR(265)             NOT NULL,
    created               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username              VARCHAR(256)             NOT NULL,
    uuid                  uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    source_uri            VARCHAR(4096)                  NOT NULL,
    destination_uri       VARCHAR(4096)                  NOT NULL,
    status                VARCHAR(128)             NOT NULL,
    bytes_transferred     BIGINT                   NOT NULL default 0,
    total_bytes           BIGINT                   NOT NULL default 0,
    final_message         VARCHAR,
    start_time            TIMESTAMP WITH TIME ZONE,
    end_time              TIMESTAMP WITH TIME ZONE,
    error_message         TEXT,
    task_id int REFERENCES transfer_tasks(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE index on transfer_tasks_parent (tenant_id, username, uuid, task_id);
CREATE INDEX ON transfer_tasks_parent (uuid);

CREATE TABLE IF NOT EXISTS transfer_tasks_child
(
    id                    serial PRIMARY KEY,
    tenant_id             VARCHAR(265)             NOT NULL,
    created               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username              VARCHAR(256)             NOT NULL,
    uuid                  uuid                     NOT NULL DEFAULT uuid_generate_v4(),
    source_uri           VARCHAR(4096)                  NOT NULL,
    destination_uri      VARCHAR(4096)                  NOT NULL,
    status                VARCHAR(128)             NOT NULL,
    bytes_transferred     BIGINT                   NOT NULL default 0,
    total_bytes           BIGINT                   NOT NULL default 0,
    retries               INT                      NOT NULL default 0,
    start_time               TIMESTAMP WITH TIME ZONE,
    end_time               TIMESTAMP WITH TIME ZONE,
    error_message         VARCHAR,
    parent_task_id int REFERENCES transfer_tasks_parent(id) ON DELETE CASCADE ON UPDATE CASCADE,
    task_id int REFERENCES transfer_tasks(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX on transfer_tasks_child (tenant_id, username, uuid, parent_task_id, task_id);
CREATE INDEX ON transfer_tasks_child (uuid);

CREATE TABLE IF NOT EXISTS shares
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
