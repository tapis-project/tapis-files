FROM maven:3-openjdk-15 AS build
EXPOSE 8080
WORKDIR /app
# Just copy the jars needed
COPY  api/target  ./api/target/
CMD ["java", "-cp", "api/target/tapis-files.jar:api/target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]



