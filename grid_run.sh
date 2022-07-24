#!/bin/bash
# INDEXORS=('idnes.cz' 'aktualne.cz' 'novinky.cz' 'seznamzpravy.cz' 'irozhlas.cz')
INDEXORS=('idnes.cz' 'aktualne.cz')
PYTHON_PATH="/opt/python/3.10.4/bin/python3"
ARTEMIS_HOST="cpu-node15"
ARTEMIS_PORT="61602"
HTTP_PORT="6100"
ARTEMIS_MEMORY_MAX="64g"

ARTEMIS_PATH="$(realpath "$(dirname "$0")"/Artemis)"
AGGREGATOR_PATH="$(realpath "$(dirname "$0")"/Aggregator)"
PROCESSOR_PATH="$(realpath "$(dirname "$0")"/Processor)"
EXC="qsub -j y -cwd"
PROCESSORS_NUM="$((5 * ${#INDEXORS[@]}))"

#Artemis
$EXC -o artemis.log -e artemis.err -q "cpu.q@$ARTEMIS_HOST" -pe smp 4 -l mem_free="$ARTEMIS_MEMORY_MAX",act_mem_free="$ARTEMIS_MEMORY_MAX" "$ARTEMIS_PATH/create_artemis.sh" "$ARTEMIS_PATH" "0.0.0.0"  "$ARTEMIS_PORT" "$HTTP_PORT" "$ARTEMIS_MEMORY_MAX"

# Aggregators
ENV_PATH="$AGGREGATOR_PATH/env"
if [[ ! -e "$ENV_PATH" ]]; then
    "$PYTHON_PATH" -m venv "$ENV_PATH"
    "$ENV_PATH/bin/pip3" install -r "$AGGREGATOR_PATH/requirements.txt"
fi

for index in "${INDEXORS[@]}"; do
    $EXC -o "aggregator_$index.log" -e "aggregator_$index.err" "$AGGREGATOR_PATH/aggregate.sh" "$AGGREGATOR_PATH" \
    --queue_host="$ARTEMIS_HOST" \
    --queue_port=$ARTEMIS_PORT \
    "$index"
done


#Processors
ENV_PATH="$PROCESSOR_PATH/env"
if [[ ! -e "$ENV_PATH" ]]; then
    "$PYTHON_PATH" -m venv "$ENV_PATH"
    "$ENV_PATH/bin/pip3" install -r "$PROCESSOR_PATH/requirements.txt"
fi

for ((i=0; i < "$PROCESSORS_NUM"; ++i)); do
    $EXC -o "processor_$i.log" -e "processor_$i.err" "$PROCESSOR_PATH/process.sh" "$PROCESSOR_PATH" \
    --queue_host="$ARTEMIS_HOST" \
    --pills_to_die="${#INDEXORS[@]}" \
    --queue_port="$ARTEMIS_PORT" \
    --output_path="$(realpath "./output_${i}")" \
    --timeout=360 \
    --extractors_path="$PROCESSOR_PATH/App/DoneExtractors" \
    --config_path="$PROCESSOR_PATH/App/config.json"
done









