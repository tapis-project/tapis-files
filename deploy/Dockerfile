##########################################################################
# Dockerfile for Tapis Applications service
# This file must be placed in the build directory (dependencies)
#   before docker build is run.
#
#   $TAG            the tag for image identification
#
##########################################################################
FROM eclipse-temurin:17-jdk-jammy

MAINTAINER CIC Support <cicsupport@tacc.utexas.edu>

# apt update
RUN apt update 

# Install less. 
RUN apt install -y less 

# Install vi.
RUN apt install -y vim

# Add user tapis
RUN useradd -m tapis

USER tapis

# Just copy the jars needed
WORKDIR /home/tapis/app
COPY  api/target/tapis-files.jar ./
COPY  api/target/dependencies ./dependencies


# Java module commands that are always needed (java 17 and later).
ENV JAVA_MODULE_OPTS --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED \
                     --add-opens java.base/java.util=ALL-UNNAMED
ENV JAVA_CMD_OPTS -Xdebug -Xmx3g -agentlib:jdwp=transport=dt_socket,server=y,address=*:8000,suspend=n 

# Server port, debug port and jmx port
EXPOSE 8080 8000 9090

CMD java ${JAVA_CMD_OPTS} ${JAVA_MODULE_OPTS} -Dlogback.configurationFile=dependencies/classes/logback.xml -cp "./tapis-files.jar:dependencies/*" edu.utexas.tacc.tapis.files.api.FilesApplication

