FROM maven:3-openjdk-15 AS build
EXPOSE 8080
WORKDIR /app
#RUN mvn -pl api clean install --also-make -DskipTests
COPY  api/target  ./api/target/
WORKDIR /app/api
CMD ["java", "-cp", "target/tapis-files.jar:target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]



