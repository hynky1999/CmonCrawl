#!/bin/bash
ARTEMIS_PATH=$1
ARTEMIS_HOST=$2
ARTEMIS_PORT=$3
HTTP_PORT=$4
ARTEMIS_MEMORY_MAX=$5
ARTEMIS_CFG="$ARTEMIS_PATH/config"
ARTEMIS_RUN_PATH="$ARTEMIS_PATH/run_artemis"
# I think this sould be se to (dirname $0)/run_artemis
# In order to get normal write speed else it is in home directory
# But I don't know how to remove it if I stop task
ARTEMIS_CREATE="$ARTEMIS_PATH/artemis-create"

echo "$ARTEMIS_RUN_PATH"

if [[ ! -e $ARTEMIS_CREATE ]]; then
    wget "https://dlcdn.apache.org/activemq/activemq-artemis/2.23.1/apache-artemis-2.23.1-bin.zip"
    unzip  "./apache-artemis-2.23.1-bin.zip"
    rm  "./apache-artemis-2.23.1-bin.zip"
    mv "apache-artemis-2.23.1" "$ARTEMIS_CREATE"
fi

if [[ ! -e $ARTEMIS_RUN_PATH ]]; then
    "$ARTEMIS_CREATE/bin/artemis" create --force --user=f --password=f --paging --require-login --java-options="-Xmx${ARTEMIS_MEMORY_MAX}" --host="$ARTEMIS_HOST" --http-host="$ARTEMIS_HOST" --default-port="$ARTEMIS_PORT" --http-port="$HTTP_PORT" --no-amqp-acceptor --no-hornetq-acceptor --no-mqtt-acceptor --no-stomp-acceptor --aio --verbose "$ARTEMIS_RUN_PATH"
    export ARTEMIS_RUN_PATH ARTEMIS_CFG
    python3 "$ARTEMIS_PATH/adjust_config.py"
fi

"${ARTEMIS_RUN_PATH}/bin/artemis" run