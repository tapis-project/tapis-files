CREATE TABLE IF NOT EXISTS files_postits
(
  id               TEXT PRIMARY KEY,
  systemId         TEXT,
  path             TEXT,
  allowedUses      int,
  timesUsed        int,
  jwtUser          TEXT,
  jwtTenantId      TEXT,
  owner            TEXT,
  tenantId         TEXT,
  expiration       TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  created          TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated          TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);
CREATE INDEX files_postits_expiration_index ON files_postits (expiration);
CREATE INDEX files_postits_owner_index ON files_postits (owner);
CREATE INDEX files_postits_tenantid_index ON files_postits (tenantId);
CREATE INDEX files_postits_timesused_index ON files_postits (timesUsed);

