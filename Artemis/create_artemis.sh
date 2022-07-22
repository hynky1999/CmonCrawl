#!/bin/bash
SCRIPT=$(realpath "$0")
ARTEMIS_PATH="$(dirname "$SCRIPT")/artemis_run"
ARTEMIS_CFG="$(dirname "$SCRIPT")/config"
export ARTEMIS_PATH
export ARTEMIS_CFG

if [[ -z $HOST ]]; then
    HOST="0.0.0.0"
fi

"$(dirname "$0")/artemis-create/bin/artemis" create --force --user=f --password=f --require-login --host="$HOST" --http-host="$HOST" --no-amqp-acceptor --no-hornetq-acceptor --no-mqtt-acceptor "${ARTEMIS_PATH}"
python3 adjust_config.py
"${ARTEMIS_PATH}"/bin/artemis run