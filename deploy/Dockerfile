FROM maven:3-openjdk-15 AS build
EXPOSE 8080
WORKDIR /app
#RUN mvn -pl api clean install --also-make -DskipTests
COPY  api/target  ./api/target/
COPY  migrations ./migrations/
CMD ["java", "-cp", "api/target/tapis-files.jar:api/target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]



