#!/bin/bash
# INDEXORS=('idnes.cz' 'aktualne.cz' 'novinky.cz' 'seznamzpravy.cz' 'irozhlas.cz')
INDEXORS=('idnes.cz')
ARTEMIS_HOST="localhost"
ARTEMIS_PORT="61602"
HTTP_PORT="8000"
ARTEMIS_MEMORY_MAX="8g"
export ARTEMIS_HOST ARTEMIS_PORT HTTP_PORT ARTEMIS_MEMORY_MAX

AGGREGATOR_EXEC="./Aggregator/aggregate.sh"
AGGREGATOR_ENV="./Aggregator/environment.yml"
PROCESSOR_EXEC="./Processor/process.sh"
PROCESSOR_ENV="./Processor/environment.yml"
ARTEMIS_EXEC="./Artemis/create_artemis.sh"
EXC="qsub -c-cwd -j y"
EXC=""
PROCESSORS_NUM=2


# Aggregators
ENV_PATH="$(realpath agg_env)"
export ENV_PATH

if [[ ! -e "$ENV_PATH" ]]; then
    conda env create --prefix "$ENV_PATH" --file "$AGGREGATOR_ENV"
fi

for index in "${INDEXORS[@]}"; do
    echo "Started Aggregator for $index"
    $EXC $AGGREGATOR_EXEC --queue_host="$ARTEMIS_HOST" --queue_port=$ARTEMIS_PORT "$index" &
done

#Processors

ENV_PATH="$(realpath proc_env)"
if [[ ! -e "$ENV_PATH" ]]; then
    conda env create --prefix "$ENV_PATH" --file "$PROCESSOR_ENV"
fi
for ((i = 0; i < "$PROCESSORS_NUM"; ++i)); do
    echo "Started Processor $i"
    $EXC $PROCESSOR_EXEC --queue_host="$ARTEMIS_HOST" --pills_to_die="${#INDEXORS[@]}" --queue_port="$ARTEMIS_PORT"  --output_path="$(realpath "./output_${i}")" &
done


#Artemis
$EXC "$ARTEMIS_EXEC" "$ARTEMIS_HOST"
pkill -P $$

    






