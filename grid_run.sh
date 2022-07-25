#!/bin/bash
INDEXORS=('idnes.cz' 'aktualne.cz' 'novinky.cz' 'denik.cz' 'seznamzpravy.cz' 'irozhlas.cz' 'ihned.cz')
PYTHON_PATH="/opt/python/3.10.4/bin/python3"
ARTEMIS_HOST="cpu-node15"
ARTEMIS_PORT="61602"
HTTP_PORT="6100"
ARTEMIS_MEMORY_MAX="64g"

CUR_DIR="$(realpath "$(dirname "$0")")"
AGG_EXEC="${CUR_DIR}/aggregate.sh"
PROC_EXEC="${CUR_DIR}/process.sh"
PROCESSOR_PATH="${CUR_DIR}/Processor"
AGGREGATOR_PATH="${CUR_DIR}/Aggregator"
ARTEMIS_PATH="$CUR_DIR/Artemis"
ARTEMIS_RUN_PATH=$ARTEMIS_PATH/run_artemis
EXC="qsub -j y -cwd"
PROCESSORS_NUM="$((7 * ${#INDEXORS[@]}))"
# If fresh start =1 then all Artemis data will be deleted
# -> duplicates will be forgotten and data lost.
# Good for testing.
FRESH_START=1

#Artemis
$EXC -o artemis.log -e artemis.err -q "cpu.q@$ARTEMIS_HOST" \
-pe smp 4 \
-l mem_free="$ARTEMIS_MEMORY_MAX",act_mem_free="$ARTEMIS_MEMORY_MAX" \
"$ARTEMIS_PATH/create_artemis.sh" \
"$ARTEMIS_PATH" \
"$ARTEMIS_RUN_PATH" \
"0.0.0.0" \
"$ARTEMIS_PORT" \
"$HTTP_PORT" \
"$ARTEMIS_MEMORY_MAX" \
"$FRESH_START"

# Aggregators
ENV_PATH="$AGGREGATOR_PATH/env"
if [[ ! -e "$ENV_PATH" ]]; then
    "$PYTHON_PATH" -m venv "$ENV_PATH"
    "$ENV_PATH/bin/pip3" install -r "$AGGREGATOR_PATH/requirements.txt"
fi

for index in "${INDEXORS[@]}"; do
    $EXC -o "aggregator_$index.log" -e "aggregator_$index.err" "$AGG_EXEC" "$CUR_DIR" \
    --queue_host="$ARTEMIS_HOST" \
    --queue_port=$ARTEMIS_PORT \
    --max_retry=50 \
    "$index"
done


#Processors
ENV_PATH="$PROCESSOR_PATH/env"
if [[ ! -e "$ENV_PATH" ]]; then
    "$PYTHON_PATH" -m venv "$ENV_PATH"
    "$ENV_PATH/bin/pip3" install -r "$PROCESSOR_PATH/requirements.txt"
fi

for ((i=0; i < "$PROCESSORS_NUM"; ++i)); do
    $EXC -o "processor_$i.log" -e "processor_$i.err" "$PROC_EXEC" "$CUR_DIR" \
    --queue_host="$ARTEMIS_HOST" \
    --pills_to_die="${#INDEXORS[@]}" \
    --queue_port="$ARTEMIS_PORT" \
    --output_path="$(realpath "./output_${i}")" \
    --timeout=360 \
    --max_retry=20 \
    --extractors_path="$PROCESSOR_PATH/App/DoneExtractors" \
    --config_path="$PROCESSOR_PATH/App/config.json"
done









