FROM maven:3-jdk-13
EXPOSE 8080
WORKDIR /app
# Copy all of the repo into the /app dir. The .dockerignore file will skip the target dirs
COPY ./api/target/ ./target
#RUN mvn -pl api clean install --also-make -DskipTests
CMD ["java", "-cp", "target/tapis-files.jar:target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]