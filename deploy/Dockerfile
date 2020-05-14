FROM maven:3-jdk-13
EXPOSE 8080
WORKDIR /app

RUN yum install git -y
# Copy all of the repo into the /app dir. The .dockerignore file will skip the target dirs
COPY . .

RUN mvn -pl api clean install --also-make -DskipTests
CMD ["java", "-cp", "api/target/tapis-files.jar:api/target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]