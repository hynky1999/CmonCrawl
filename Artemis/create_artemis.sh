#!/bin/bash
ARTEMIS_PATH=$1
ARTEMIS_HOST=$2
ARTEMIS_PORT=$3
HTTP_PORT=$4
ARTEMIS_MEMORY_MAX=$5
ARTEMIS_CFG="$ARTEMIS_PATH/config"
#ARTEMIS_RUN_PATH="$ARTEMIS_PATH/run_artemis"
ARTEMIS_RUN_PATH="./run_artemis"
ARTEMIS_CREATE="$ARTEMIS_PATH/artemis-create"
export ARTEMIS_CFG ARTEMIS_RUN_PATH


if [[ ! -e $ARTEMIS_CREATE ]]; then
    wget "https://dlcdn.apache.org/activemq/activemq-artemis/2.23.1/apache-artemis-2.23.1-bin.zip"
    unzip  "./apache-artemis-2.23.1-bin.zip"
    rm  "./apache-artemis-2.23.1-bin.zip"
    mv "apache-artemis-2.23.1" "$ARTEMIS_CREATE"
fi
"$ARTEMIS_CREATE/bin/artemis" create --force --user=f --password=f --paging --require-login --java-options="-Xmx${ARTEMIS_MEMORY_MAX}" --host="$ARTEMIS_HOST" --http-host="$ARTEMIS_HOST" --default-port="$ARTEMIS_PORT" --http-port="$HTTP_PORT" --no-amqp-acceptor --no-hornetq-acceptor --no-mqtt-acceptor --no-stomp-acceptor --aio --verbose "$ARTEMIS_RUN_PATH"


python3 "$ARTEMIS_PATH/adjust_config.py"
artemis run
"${ARTEMIS_RUN_PATH}/bin/artemis" run
rm -rf "$ARTEMIS_RUN_PATH"