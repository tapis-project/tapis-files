#!/bin/sh
# Use mvn to run the integration tests in the lib directory.
# The argument p6spy.config.modulelist= turns off debug logging of SQL calls.
#   Simply remove the argument to see SQL calls
#
# To run against DEV environment services (tenants, tokens) and a local DB
#   with default DB username and password as set in service.properties then
#   set the following:
#     TENANTS_SERVICE_URL=https://admin.develop.tapis.io
#     TAPIS_SERVICE_PASSWORD=******
#
# In general the following env variables should be set prior to running this script:
#   DB_USERNAME
#   DB_PASSWORD
#   DB_NAME
#   DB_HOST
#   DB_PORT
#   TENANT_SERVICE_URL
#   TAPIS_SERVICE_PASSWORD
# For example:
#   DB_USERNAME=tapis
#   DB_PASSWORD=******
#   DB_NAME=test
#   DB_HOST=localhost
#   DB_PORT=5432
#   TENANT_SERVICE_URL=https://admin.develop.tapis.io
#   TAPIS_SERVICE_PASSWORD=******
#
# The following env variables must be set
#   TENANT_SERVICE_URL
#   TAPIS_SERVICE_PASSWORD
#
# If env variables are not set then these defaults will be used:
# (see service.properties for defaults)
#   DB_USERNAME=test
#   DB_PASSWORD=test
#   DB_NAME=test
#   DB_HOST=localhost
#   DB_PORT=5432
#   TENANT_SERVICE_URL=https://dev.develop.tapis.io
#   TAPIS_SERVICE_PASSWORD=******

# Determine absolute path to location from which we are running
#  and change to that directory
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

cd ../
# this command will stop after any module that has an error.  To get maven to run tests for
# all modules even if there is a test failure, add "-fn" argumnent.  The default is -foe
# (fail never instead of fail on error).  For example:
#     mvn verify -Ptest -fn -Dp6spy.config.modulelist=
mvn verify -Ptest -Dp6spy.config.modulelist=

LIB_RET_CODE=$?
cd $RUN_DIR

exit $LIB_RET_CODE
