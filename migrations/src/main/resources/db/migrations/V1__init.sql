CREATE table transfer_tasks (
    id serial PRIMARY KEY,
    tenant_id VARCHAR(265) NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(256) NOT NULL,
    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
    source_system_id VARCHAR NOT NULL,
    source_path VARCHAR NOT NULL,
    destination_system_id VARCHAR NOT NULL,
    destination_path VARCHAR NOT NULL,
    status VARCHAR(128) NOT NULL,
)
CREATE index on transfer_tasks(tenant_id, username, uuid);


CREATE TABLE shares (
    id serial PRIMARY KEY,
    tenant_id VARCHAR(265) NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(256) NOT NULL,
    shared_with_username VARCHAR(256) NOT NULL,
    system_id VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
    max_views integer NOT NULL,
    views integer NOT NULL
)
CREATE index on shares(tenant_id, username, uuid);

CREATE TABLE permissions (
    id serial PRIMARY KEY,
    tenant_id VARCHAR(265) NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(256) NOT NULL,
    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
    system_id VARCHAR NOT NULL,
    path VARCHAR NOT NULL
)
CREATE index on permissions(tenant_id, username, uuid);
