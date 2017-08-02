FROM java:openjdk-8-jre-alpine

LABEL "catalogue.project_name"="datatools-server"

ENV SLEEP_TIME 0

# add directly the jar
ADD dt-*.jar /datatools-server.jar

RUN sh -c 'touch /datatools-server.jar'
RUN sh -c 'mkdir /conf'

VOLUME /conf
VOLUME /tmp

EXPOSE 4200

CMD echo "The application will start in ${SLEEP_TIME}s..." && \
    sleep ${SLEEP_TIME} && \
    java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -jar /datatools-server.jar /conf/env.yml /conf/server.yml
