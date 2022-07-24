# Using jdk as base image
FROM openjdk:20-slim-bullseye
RUN apt-get update && apt-get install -y python3&& apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
COPY . .
ENV ARTEMIS_PATH="/var/lib/artemis"
ENV ARTEMIS_CFG="./config"
# Retarded config it will be overwrite by config anyway so user and pass are meaningless
RUN artemis-create/bin/artemis create --user=f --password=f --java-options="-Xmx8G" --require-login --http-host="0.0.0.0" --host="0.0.0.0" --paging --no-amqp-acceptor --no-hornetq-acceptor --no-mqtt-acceptor ${ARTEMIS_PATH}
RUN cp -f config/* ${ARTEMIS_PATH}/etc
RUN python3 adjust_config.py
EXPOSE 8161
CMD "${ARTEMIS_PATH}/bin/artemis" run