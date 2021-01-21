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
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
create database test;
create user test with encrypted password 'test';
alter user test WITH SUPERUSER;   
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

```
╰─$ mvn -pl migrations flyway:clean flyway:migrate -Dflyway.url=jdbc:postgresql://localhost:5432/dev -Dflyway.user=dev -Dflyway.password=dev -U
```

### Hit the API
You should also be able to hit the API from postman at 
`localhost:8080/v3/files/ops/{systemID}/{path}` 

Profit





