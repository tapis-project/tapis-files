#!/bin/bash
#docker compose up -d

SCRIPT_DIR=$(dirname $0)

http POST http://localhost:8088/v3/systems $AUTH < ${SCRIPT_DIR}/jsonFiles/ssh-1.json 
http POST http://localhost:8088/v3/systems $AUTH < ${SCRIPT_DIR}/jsonFiles/ssh-2.json 
http POST http://localhost:8088/v3/systems $AUTH < ${SCRIPT_DIR}/jsonFiles/irods-1.json 

http POST http://localhost:8088/v3/systems/credential/tapisv3-ssh-1/user/testuser2?skipCredentialCheck=true $AUTH  < ${SCRIPT_DIR}/jsonFiles/ssh-creds.json
http POST http://localhost:8088/v3/systems/credential/tapisv3-ssh-2/user/testuser2?skipCredentialCheck=true $AUTH  < ${SCRIPT_DIR}/jsonFiles/ssh-creds.json
http POST http://localhost:8088/v3/systems/credential/tapisv3-irods-1/user/testuser2?skipCredentialCheck=true $AUTH  < ${SCRIPT_DIR}/jsonFiles/irods-creds.json

