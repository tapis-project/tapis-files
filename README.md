# tapis-files

### Setup steps

In the `deploy` directory startup the necessary containers.

```docker-compose up```

##### IRODS setup
We need to create an irods user for testing
Exec into the irods container

```docker exec -it irods bash```

Run admin commands to add the user

``` 
iinit (host=localhost, user=rods, passwd=rods)
iadmin mkuser dev rodsuser
iadmin moduser dev password dev
```


##### Database setup
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

### Run a build

```$xslt
mvn clean install
```

### Run tests

The integration tests are configured to use the `test` database created above.

```
mvn clean install -DskipITs=false -DAPP_ENV=test
```

### Run migrations
Tests should run migrations, to run them manually for any DB do this:
```
╰─$ mvn -pl migrations flyway:clean flyway:migrate -Dflyway.skip=false -Dflyway.url=jdbc:postgresql://localhost:5432/dev -Dflyway.user=dev -Dflyway.password=dev -U
```
### Start the service
Start the service from the IDE, class edu.utexas.tacc.tapis.files.api.FilesApplication,
in intellij env vars config, set SERVICE_PASSWORD

### Hit the API
You should also be able to hit the API from postman at 
`localhost:8080/v3/files/ops/{systemID}/{path}` 

Profit 

### specCleaner.sh 

There is (was?) an issue with the python sdk generation where it would error out if the
status codes in the openapi specs were integers and not strings. There is a little script called
`specCleaner.sh` in the resources of the api module that cleans the spec up. This is run automatically
as part of the maven build process. 





