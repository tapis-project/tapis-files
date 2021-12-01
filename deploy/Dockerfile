##########################################################################
# Dockerfile for Tapis Applications service
# This file must be placed in the build directory (api/target)
#   before docker build is run.
#
#   $TAG            the tag for image identification
#
##########################################################################
FROM openjdk:15-jdk-oracle

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

WORKDIR /app
# Just copy the jars needed
COPY  api/target  ./api/target/
WORKDIR /app/api
# Server port, debug port and jmx port
EXPOSE 8080
EXPOSE 8000
EXPOSE 9090

CMD ["java", "-Xms4g", "-Xmx4g", "-cp", "target/tapis-files.jar:target/dependencies/*", "edu.utexas.tacc.tapis.files.api.FilesApplication"]



