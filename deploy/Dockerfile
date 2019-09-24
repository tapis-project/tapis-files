FROM maven:3-jdk-12
EXPOSE 8080
WORKDIR /app
RUN ls -la
RUN pwd
# Copy main pom file and shared libs
COPY . .

RUN mvn -pl api clean install --also-make -DskipTests

CMD ["java", "-cp", "/app/api,/app/lib", "edu.utexas.tacc.tapis.files.api.FilesApplication"]