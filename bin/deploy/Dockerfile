FROM maven:3-jdk-12
EXPOSE 8080
WORKDIR /app

# Copy all of the repo into the /app dir. The .dockerignore file will skip the target dirs
COPY . .

RUN mvn -pl api clean install --also-make -DskipTests

CMD ["java", "-jar", "api/target/tapis-files.jar"]