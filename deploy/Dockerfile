FROM maven:3-openjdk-15 AS build
RUN microdnf install -y git
EXPOSE 8080
WORKDIR /app
# Copy all of the repo into the /app dir. The .dockerignore file will skip the target dirs
COPY pom.xml .
COPY api/pom.xml ./api/pom.xml
COPY lib/pom.xml ./lib/pom.xml
COPY migrations/pom.xml ./migrations/pom.xml
COPY notifications/pom.xml ./notifications/pom.xml
# should only install the deps
RUN mvn clean install --fail-never
#now copy all the source and build
COPY . .
RUN mvn package -DskipTests

FROM openjdk:15-slim
WORKDIR /app
#RUN mvn -pl api clean install --also-make -DskipTests
COPY --from=build /app/api ./api
WORKDIR /app/api
CMD ["java", "-cp", "target/tapis-files.jar:target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]