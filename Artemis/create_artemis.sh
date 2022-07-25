#!/bin/bash
ARTEMIS_PATH=$1
ARTEMIS_RUN_PATH=$2
ARTEMIS_HOST=$3
ARTEMIS_PORT=$4
HTTP_PORT=$5
ARTEMIS_MEMORY_MAX=$6
FRESH_START=$7
ARTEMIS_CFG="$ARTEMIS_PATH/config"
# I think this sould be se to (dirname $0)/run_artemis
# In order to get normal write speed else it is in home directory
# But I don't know how to remove it if I stop task
ARTEMIS_CREATE="$ARTEMIS_PATH/artemis-create"


if [[ ! -e $ARTEMIS_CREATE ]]; then
    wget "https://dlcdn.apache.org/activemq/activemq-artemis/2.23.1/apache-artemis-2.23.1-bin.zip"
    unzip  "./apache-artemis-2.23.1-bin.zip"
    rm  "./apache-artemis-2.23.1-bin.zip"
    mv "apache-artemis-2.23.1" "$ARTEMIS_CREATE"
fi

if [[ ! -e $ARTEMIS_RUN_PATH ]]; then
    "$ARTEMIS_CREATE/bin/artemis" create \
    --force \
    --user=f \
    --password=f \
    --paging --require-login \
    --java-options="-Xmx${ARTEMIS_MEMORY_MAX}" \
    --host="$ARTEMIS_HOST" \
    --http-host="$ARTEMIS_HOST" \
    --default-port="$ARTEMIS_PORT" \
    --http-port="$HTTP_PORT" \
    --no-amqp-acceptor \
    --no-hornetq-acceptor \
    --no-mqtt-acceptor \
    --no-stomp-acceptor \
    --aio \
    --verbose \
    "$ARTEMIS_RUN_PATH"
    
    export ARTEMIS_RUN_PATH ARTEMIS_CFG
    python3 "$ARTEMIS_PATH/adjust_config.py"
fi

if [[ $FRESH_START -eq 1 ]]; then
    rm -r "$ARTEMIS_RUN_PATH/data"
fi

"${ARTEMIS_RUN_PATH}/bin/artemis" run