FROM openjdk:15 AS build
EXPOSE 8080
WORKDIR /app
# Just copy the jars needed
COPY  api/target  ./api/target/
WORKDIR /app/api
CMD ["java", "-cp", "target/tapis-files.jar:target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]



