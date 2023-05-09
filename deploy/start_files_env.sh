#!/bin/bash
set -xv

POSTGRES_CONTAINER="tapis-postgres"
POSTGRES_USER="dev"
POSTGRES_PASSWORD="dev"

RABBIT_TEST_CONTAINER_NAME="tapis-rabbitmq"
RABBIT_TEST_VHOST="testvhost"
RABBIT_TEST_USER="testuser"
RABBIT_TEST_PASSWORD="testpass"

IRODS_CONTAINER="irods"
IRODS_ADMIN_USER="rods"
IRODS_ADMIN_PASSWORD="rods"
IRODS_USER="dev"
IRODS_PASSWORD="dev"

#this user/password is different than the ones above
docker exec -i ${IRODS_CONTAINER} iinit <<EOD
localhost
${IRODS_ADMIN_USER}
${IRODS_ADMIN_PASSWORD}
EOD

docker exec -i ${IRODS_CONTAINER} iadmin mkuser ${IRODS_USER} rodsuser
docker exec -i ${IRODS_CONTAINER} iadmin moduser ${IRODS_USER} password ${IRODS_PASSWORD}
docker exec -i ${POSTGRES_CONTAINER} psql -U ${POSTGRES_USER} filesdb <<EOD

create database test;
create user test with encrypted password 'test';
alter user test WITH SUPERUSER;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
EOD

RABBITMQCTL_CMDS=$(
cat <<EOD
rabbitmqctl add_vhost ${RABBIT_TEST_VHOST}
rabbitmqctl add_user ${RABBIT_TEST_USER} ${RABBIT_TEST_PASSWORD} 
rabbitmqctl set_permissions ${RABBIT_TEST_USER} --vhost ${RABBIT_TEST_VHOST} ".*" ".*" ".*" 
rabbitmqctl set_user_tags ${RABBIT_TEST_USER} administrator
EOD
)
set -xv
docker exec -it --privileged ${RABBIT_TEST_CONTAINER_NAME} /bin/bash -lic "$RABBITMQCTL_CMDS"
