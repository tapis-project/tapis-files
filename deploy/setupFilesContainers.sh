#!/bin/bash
#set -xv

SCRIPT_DIR=$(dirname $0)

SERVICE_CODE="files"

PG_PORT="5432"
PG_ADMIN_USER="dev"
PG_CONTAINER="tapis_${SERVICE_CODE}_postgres"
PG_USER="tapis_${SERVICE_CODE}"
#PG_SCHEMA="tapis_${SERVICE_CODE}"
PG_SCHEMA="public"
PG_DATABASE="tapis${SERVICE_CODE}db"
PG_PASSWORD="password"
PG_TEST_USER="test"
PG_TEST_PASSWORD="test"
PG_TEST_DB="test"

RABBIT_CONTAINER_NAME="tapis-rabbitmq"
RABBIT_VHOST="rabbit"
RABBIT_USER="tapis"
RABBIT_PASSWORD="password"

IRODS_CONTAINER="irods-1"
IRODS_ADMIN_USER="rods"
IRODS_ADMIN_PASSWORD="rods"
IRODS_USER="dev"
IRODS_PASSWORD="dev"

function usage() {
  echo "$0 [-t tenant] [-p password] [-u user] [--dev | --prod | --staging]"

  echo "OPTIONS:"
  echo "     -p --port"
  echo "        The port to run postgres on"
  echo 
  echo "     -u --pguser"
  echo "        The postgres user for the service"
  echo 
  echo "     -w --pgpass"
  echo "        The postgres password for the service"
  echo 
  echo "     -d --pgdb"
  echo "        The postgres database name for the service"
  echo 
  echo "     -s --pgschema"
  echo "        The postgres schema for the service"
  echo 
  exit 1
}

function announce() {
  echo ---==== $@ ====---
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--port)
      PG_PORT="$2"
      shift # past argument
      shift # past value
      ;;
    -u|--pguser)
      PG_USER="$2"
      shift # past argument
      shift # past value
      ;;
    -w|--pgpass)
      PG_PASSWORD="$2"
      shift # past argument
      shift # past value
      ;;
    -d|--pgdb)
      PG_DATABASE="$2"
      shift # past argument
      shift # past value
      ;;
    -s|--pgschema)
      PG_SCHEMA="$2"
      shift # past argument
      shift # past value
      ;;
    -*|--*)
      echo "Unknown option $1"
      usage
      ;;
    *)
      echo "Unknown positional arguement $1"
      usage
  esac
done

announce "database container on port ${PG_PORT}"
export PG_PORT

announce "running docker compose up"
docker compose -f ${SCRIPT_DIR}/docker-compose.yml up --wait

announce "pausing for startup"
sleep 5

announce "setting up rabbitmq"
RABBITMQCTL_CMDS=$(
cat <<EOD
set -xv
rabbitmqctl add_vhost ${RABBIT_VHOST}
rabbitmqctl add_user ${RABBIT_USER} ${RABBIT_PASSWORD} 
rabbitmqctl set_permissions ${RABBIT_USER} --vhost ${RABBIT_VHOST} ".*" ".*" ".*" 
rabbitmqctl set_user_tags ${RABBIT_USER} administrator
EOD
)

#For some reason it doesnt work to put this in the above block.  It's not strictly required, but there is some crap on the disk that causes rabbit to see some extra queues and stuff.
#if that's an issue, login to the container with:
#   docker exec  -it --privileged tapis-rabbitmq /bin/bash
#replace "tapis-rabbitmq" with the proper name if you changed it
#this do this command from bash in the container:
#   rabbitmqadmin --username ${RABBIT_USER} --password ${RABBIT_PASSWORD} --vhost ${RABBIT_VHOST} list queues name | awk '{print $2}' | xargs -I qn rabbitmqadmin --user ${RABBIT_USER} --password ${RABBIT_PASSWORD} --vhost ${RABBIT_VHOST} delete queue name=qn
#Using default parameters it expands to:
#   rabbitmqadmin --username tapis --password password --vhost rabbit list queues name | awk '{print $2}' | xargs -I qn rabbitmqadmin --user tapis --password password --vhost rabbit delete queue name=qn

docker exec -it --privileged ${RABBIT_CONTAINER_NAME} /bin/bash -lic "$RABBITMQCTL_CMDS"

announce "pause a moment to ensure database is running ok"
sleep 2

announce "create database and user"

announce "create schema and set privileges"
docker exec -i ${PG_CONTAINER} psql -U ${PG_ADMIN_USER} <<EOD
SELECT 'CREATE DATABASE ${PG_DATABASE} ENCODING="UTF8" LC_COLLATE="en_US.utf8" LC_CTYPE="en_US.utf8" ' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${PG_DATABASE}')\gexec
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO public;
EOD

announce "create test database"
docker exec -i ${PG_CONTAINER} psql -U ${PG_ADMIN_USER} <<EOD
SELECT 'CREATE DATABASE ${PG_TEST_DB} ENCODING="UTF8" LC_COLLATE="en_US.utf8" LC_CTYPE="en_US.utf8" ' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${PG_TEST_DB}')\gexec
CREATE SCHEMA IF NOT EXISTS public;
  GRANT ALL ON SCHEMA public TO public;
EOD

announce "create test user"
docker exec -i ${PG_CONTAINER} psql -U ${PG_ADMIN_USER} <<EOD
DO \$\$
BEGIN
  CREATE USER ${PG_TEST_USER} with encrypted password '${PG_TEST_PASSWORD}';
  EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'User already exists. User name: "${PG_TEST_USER}"';
END
\$\$
EOD

announce "set test user privs"
docker exec -i ${PG_CONTAINER} psql -U ${PG_ADMIN_USER} <<EOD
ALTER USER ${PG_TEST_USER} WITH SUPERUSER;
EOD

announce "running docker compose up"
docker compose -f ${SCRIPT_DIR}/testContainers/docker-compose-testContainers.yml up --wait

sleep 5

announce "setting up irods container"
docker exec -i ${IRODS_CONTAINER} iinit <<EOD
localhost
${IRODS_ADMIN_USER}
${IRODS_ADMIN_PASSWORD}
EOD

announce "creating irods user"
docker exec -i ${IRODS_CONTAINER} iadmin mkuser ${IRODS_USER} rodsuser
docker exec -i ${IRODS_CONTAINER} iadmin moduser ${IRODS_USER} password ${IRODS_PASSWORD}

announce "creating ssh-1 users and groups"
docker exec -i ssh-1 /bin/bash << EOF
mkdir -p /data/home/testuser/
chown -R testuser:testuser /data/home/testuser/
apt update
apt-get install acl
groupadd faclgrp1
groupadd faclgrp2
groupadd faclgrp3
useradd -g faclgrp1 facluser1
useradd -g faclgrp1 facluser2
useradd -g faclgrp1 facluser3
EOF

announce "creating ssh-2 users and groups"
docker exec -i ssh-2 /bin/bash << EOF
mkdir -p /data/home/testuser/
chown -R testuser:testuser /data/home/testuser/
apt update
apt-get install acl
groupadd faclgrp1
groupadd faclgrp2
groupadd faclgrp3
useradd -g faclgrp1 facluser1
useradd -g faclgrp1 facluser2
useradd -g faclgrp1 facluser3
EOF
