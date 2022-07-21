#!/bin/bash
ARTEMIS_PATH="./artemis_run"
ARTEMIS_CFG="./config"
if [[ ! -e "$ARTEMIS_PATH" ]]; then
    ./artemis-create/bin/artemis create --user=f --password=f --require-login --http-host="$1" ${ARTEMIS_PATH}
fi
cp -f ${ARTEMIS_CFG} ${ARTEMIS_PATH}/etc
"${ARTEMIS_PATH}"/bin/artemis run