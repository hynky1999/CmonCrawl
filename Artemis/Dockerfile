# Using jdk as base image
FROM openjdk:20-slim-bullseye
RUN apt-get update && apt-get install -y pip && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /opt
COPY . .
ENV ARTEMIS_PATH="/var/lib/artemis"
# Retarded config it will be overwrite by config anyway so user and pass are meaningless
RUN artemis-create/bin/artemis create --user=f --password=f --require-login --http-host="0.0.0.0" ${ARTEMIS_PATH}
RUN cp -f config/* ${ARTEMIS_PATH}/etc
EXPOSE 8161
CMD "${ARTEMIS_PATH}/bin/artemis" run