#!/bin/bash
#set -xv

SERVICE_CODE="files"

PG_PORT="5432"
PG_ADMIN_USER="postgres"
PG_CONTAINER="tapis_${SERVICE_CODE}_postgres"
PG_USER="tapis_${SERVICE_CODE}"
PG_SCHEMA="tapis_${SERVICE_CODE}"
PG_DATABASE="tapis${SERVICE_CODE}db"
PG_PASSWORD="password"

RABBIT_CONTAINER_NAME="tapis-rabbitmq"
RABBIT_VHOST="rabbit"
RABBIT_USER="tapis"
RABBIT_PASSWORD="password"

IRODS_CONTAINER="irods"
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
docker compose up --wait

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

docker exec -it --privileged ${RABBIT_CONTAINER_NAME} /bin/bash -lic "$RABBITMQCTL_CMDS"

announce "pause a moment to ensure database is running ok"
sleep 2

announce "create database and user"

announce "create schema and set privileges"
docker exec -i ${PG_CONTAINER} psql -U ${PG_ADMIN_USER} <<EOD
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO public;
EOD

announce "setting up irods container"
docker exec -i ${IRODS_CONTAINER} iinit <<EOD
localhost
${IRODS_ADMIN_USER}
${IRODS_ADMIN_PASSWORD}
EOD

announce "creating irods user"
docker exec -i ${IRODS_CONTAINER} iadmin mkuser ${IRODS_USER} rodsuser
docker exec -i ${IRODS_CONTAINER} iadmin moduser ${IRODS_USER} password ${IRODS_PASSWORD}

