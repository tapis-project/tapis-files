# tapis-files

### Setup steps

In the `deploy` directory startup the necessary containers.

```docker-compose up```

We need to create a test database and user for integration tests also. A dev database
is created automatically, but the test database will get wiped out after each test run. 


##### Database setup

Exec into the postgres container

```docker exec -it deploy_postgres_1 bash```

Get into a postgres shell. The default username is dev

```# psql -U dev```

Run the following commands in the postgres shell.

``` 
# create database test;
# create user test with encrypted password 'test';
# alter user test WITH SUPERUSER;   
```

### Run a build

```$xslt
mvn clean install
```

### Run tests

The integration tests are configured to use the `test` database created above.

```
docker exec -it deploy_api_1 mvn -P integration-test verify
```

### Run migrations

```
docker exec -it deploy_api_1 mvn -pl migrations flyway:migrate
```

### Hit the API
You should also be able to hit the API from postman at 
`localhost:8080/ops/{systemID}/{path}` 

Profit





