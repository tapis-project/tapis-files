##########################################################################
# Dockerfile for Tapis Applications service
# This file must be placed in the build directory (api/target)
#   before docker build is run.
#
#   $TAG            the tag for image identification
#
##########################################################################
FROM openjdk:17-jdk-bullseye

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

WORKDIR /app
# Just copy the jars needed
COPY  api/target  ./api/target/
WORKDIR /app/api

# Java module commands that are always needed (java 17 and later).
ENV JAVA_MODULE_OPTS --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED \
                     --add-opens java.base/java.util=ALL-UNNAMED
ENV JAVA_CMD_OPTS -Xdebug -Xmx3g -agentlib:jdwp=transport=dt_socket,server=y,address=*:8000,suspend=n 

# Server port, debug port and jmx port
EXPOSE 8080 8000 9090

CMD java ${JAVA_CMD_OPTS} ${JAVA_MODULE_OPTS} -Dlogback.configurationFile=target/classes/logback.xml -cp "target/tapis-files.jar:target/dependencies/*" edu.utexas.tacc.tapis.files.api.FilesApplication

