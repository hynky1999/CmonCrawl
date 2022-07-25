# Using jdk as base image
FROM openjdk:20-slim-bullseye
RUN apt-get update && \
    apt-get install -y python3 && \
    apt-get install -y wget && \
    apt-get install -y unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
RUN wget "https://dlcdn.apache.org/activemq/activemq-artemis/2.23.1/apache-artemis-2.23.1-bin.zip" && \
    unzip apache-artemis-2.23.1-bin.zip && \
    mv apache-artemis-2.23.1 artemis-create && \
    rm apache-artemis-2.23.1-bin.zip
COPY . .
ENV ARTEMIS_PATH="/var/lib/artemis"
ENV ARTEMIS_CFG="./config"
# Retarded config it will be overwrite by config anyway so user and pass are meaningless
RUN artemis-create/bin/artemis create \
    --user=f \
    --password=f \
    --java-options="-Xmx8G" \
    --require-login \
    --http-host="0.0.0.0" \
    --host="0.0.0.0" \
    --paging \
    --no-amqp-acceptor \
    --no-hornetq-acceptor \
    --no-mqtt-acceptor \
    --no-stomp-acceptor \
    --aio \
    --verbose \
    ${ARTEMIS_PATH}

RUN cp -f config/* ${ARTEMIS_PATH}/etc
RUN python3 adjust_config.py
EXPOSE 8161
CMD "${ARTEMIS_PATH}/bin/artemis" run