ALTER TABLE files_postits RENAME COLUMN alloweduses TO allowed_uses;
ALTER TABLE files_postits RENAME COLUMN timesused TO times_used;
ALTER TABLE files_postits RENAME COLUMN systemid TO system_id;
ALTER TABLE files_postits RENAME COLUMN jwtuser TO jwt_user;
ALTER TABLE files_postits RENAME COLUMN jwttenantid TO jwt_tenant_id;
ALTER TABLE files_postits RENAME COLUMN tenantid TO tenant_id;