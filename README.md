# tapis-files

Tapis Files Service

There are three primary branches: *local*, *dev*, and *main*.

All changes should first be made in the branch *local*.

When it is time to deploy to the **DEV** kubernetes environment
run the jenkins job TapisJava->3_ManualBuildDeploy->files.

This job will:
* Merge changes from *local* to *dev*
* Build, tag and push docker images
* Deploy to **DEV** environment
* Push the merged *local* changes to the *dev* branch.

To move docker images from **DEV** to **STAGING** run the following jenkins job:
* TapisJava->2_Release->promote-dev-to-staging

To move docker images from **STAGING** to **PROD** run the following jenkins job:
* TapisJava->2_Release->promote-staging-to-prod-ver


## Setup steps

Start up the supporting containers using the docker-compose file in the `deploy` directory.

```docker-compose up```

### IRODS setup
We need to create an irods user for testing
Exec into the irods container

```docker exec -it irods bash```

Run admin commands to add the user

``` 
iinit
iadmin mkuser dev rodsuser
iadmin moduser dev password dev
```

Inputs for iinit: host=localhost, user=rods, passwd=rods

### Database setup
We need to create a test database and user for integration tests also. A dev database
is created automatically, but the test database will get wiped out after each test run.

Exec into the postgres container

```docker exec -it postgres bash```

Get into a postgres shell. The default username is dev

```# psql -U dev```

Run the following commands in the postgres shell.

``` 
# create database test;
# create user test with encrypted password 'test';
# alter user test WITH SUPERUSER;   
# CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

## Run a build

```
mvn clean install
```

## Run tests

The integration tests are configured to use the `test` database created above.

Unfortunately TestTransfers takes a long time to run. And some tests in TestTransfers fail when all tests are
  run from command line, even though they pass individually when run from IDE.
When a git commit message says "Integ tests pass" or "Most integ tests pass" it usually means all tests except
  TestTransfers were run from the IDE and a few tests from TestTransfers were run from the IDE.

Currently, tests run using mvn from the command line appear to have concurrency issues. They sometimes fail.
Tests to run manually from IDE:
 - TestOpsRoutes (1.5 minutes)
 - TestFileOpsService (1 minute)
 - TestFileShareService (1 minute)
 - TestContentsRoutes (3 minutes)
 - TestLibUtilsRoutes (5 seconds)
 - TestSSHConnectionCache (10 seconds)
 - TestIrodsClient (10 seconds)
 - TestS3Client (5 seconds)
 - TestTransfersRoutes (5 seconds)
 - TestFileTransfersDAO (5 seconds)
 - TestTransfers (approx 17 minutes, intermittent fails - test10Files, testDoesTransferAtRoot, testMultipleChildren, testNestedDirectories, testSameSystemForSourceAndDest)
 -   intermittent failures succeed when run individually from IDE

```
mvn clean install -DskipITs=false -DAPP_ENV=test
```

## Run migrations
Tests should run migrations, to run them manually for any DB do this:
```
mvn -pl migrations flyway:clean flyway:migrate -Dflyway.skip=false -Dflyway.url=jdbc:postgresql://localhost:5432/dev -Dflyway.user=dev -Dflyway.password=dev -U
```

## Start the service
Start the service from the IDE, class edu.utexas.tacc.tapis.files.api.FilesApplication,
in intellij env vars config, set SERVICE_PASSWORD

## Hit the API
You should also be able to hit the API from postman at 
`localhost:8080/v3/files/ops/{systemId}/{path}` 

## specCleaner.sh 

There is (was?) an issue with the python sdk generation where it would error out if the
status codes in the openapi specs were integers and not strings. There is a little script called
`specCleaner.sh` in the resources of the api module that cleans the spec up. This is run automatically
as part of the maven build process. 

## jooq information
Any time a change is made to any database tables, new jooq classes will need to be generated.  Once the schema changes have been applied
to the database the jooq code can be generated.  This is done with the following command:

```
mvn clean install -Pdb-update
```

There is a set of default credentials/db in the mvn file for development purposes, but you can easily set those as needed with the 
following properties:

```
db.url        >jdbc:postgresql://localhost:5432/dev</db.url>
db.username   >dev</db.username>
db.password   >dev</db.password>
db.schema     >public</db.schema>
```

for example
```
mvn -Ddb.username=myuser -Ddb.password=mypassword clean install -Pdb-update
```
or 
```
mvn -Ddb.url=jdbc:postgresql://localhost:5432/mydbname -Ddb.username=myuser -Ddb.password=mypassword -Ddb.schema=myschema clean install -Pdb-update
```

Once the jooq files are generated, they can be git added.

