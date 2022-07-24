#!/bin/bash
SCRIPT=$(realpath "$0")
ARTEMIS_PATH="$(dirname "$SCRIPT")/artemis_run"


ARTEMIS_CFG="$(dirname "$SCRIPT")/config"
export ARTEMIS_CFG ARTEMIS_PATH

if [[ -z $ARTEMIS_HOST ]]; then
    ARTEMIS_HOST="0.0.0.0"
fi
if [[ -z $ARTEMIS_MEMORY_MAX ]]; then
    ARTEMIS_MEMORY_MAX="8g"
fi

if [[ -z $ARTEMIS_PORT ]]; then
    ARTEMIS_PORT="61613"
fi

if [[ -z $HTTP_PORT ]]; then
    HTTP_PORT="8161"
fi
rm -rf "$ARTEMIS_PATH"
"$(dirname "$SCRIPT")/artemis-create/bin/artemis" create --force --user=f --password=f --paging --require-login --java-options="-Xmx${ARTEMIS_MEMORY_MAX}" --host="$ARTEMIS_HOST" --http-host="$ARTEMIS_HOST" --default-port="$ARTEMIS_PORT" --http-port="$HTTP_PORT" --no-amqp-acceptor --no-hornetq-acceptor --no-mqtt-acceptor --no-stomp-acceptor "${ARTEMIS_PATH}"
python3 "$(dirname "$SCRIPT")/adjust_config.py"
"${ARTEMIS_PATH}/bin/artemis" run