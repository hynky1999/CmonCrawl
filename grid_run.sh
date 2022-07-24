#!/bin/bash
# INDEXORS=('idnes.cz' 'aktualne.cz' 'novinky.cz' 'seznamzpravy.cz' 'irozhlas.cz')
INDEXORS=('idnes.cz')
ARTEMIS_HOST="localhost"
ARTEMIS_PORT="61602"
HTTP_PORT="8080"
ARTEMIS_MEMORY_MAX="8g"
ARTEMIS_PATH="$(realpath ./Artemis)"

AGGREGATOR_EXEC="./Aggregator/aggregate.sh"
AGGREGATOR_ENV="./Aggregator/environment.yml"
PROCESSOR_EXEC="./Processor/process.sh"
PROCESSOR_ENV="./Processor/environment.yml"
# PROCESSOR_CONFIG="./Processor/App/config.json"
#EXC="qsub -j y"
EXC=""
PROCESSORS_NUM=2


# Aggregators
ENV_PATH="$(realpath agg_env)"
export ENV_PATH

if [[ ! -e "$ENV_PATH" ]]; then
    # Conda not working
    # conda env create --prefix "$ENV_PATH" --file "$AGGREGATOR_ENV"
    python3 -m venv "$ENV_PATH"
    source "${ENV_PATH}/bin/activate"
    pip3 install -r "$AGGREGATOR_ENV"
fi

for index in "${INDEXORS[@]}"; do
    echo "Started Aggregator for $index"
    $EXC $AGGREGATOR_EXEC "$ENV_PATH" --queue_host="$ARTEMIS_HOST" --queue_port=$ARTEMIS_PORT --limit=400 "$index"
done

# #Processors

# ENV_PATH="$(realpath proc_env)"
# if [[ ! -e "$ENV_PATH" ]]; then
#     conda env create --prefix "$ENV_PATH" --file "$PROCESSOR_ENV"
# fi
# for ((i = 0; i < "$PROCESSORS_NUM"; ++i)); do
#     echo "Started Processor $i"
#     $EXC $PROCESSOR_EXEC --queue_host="$ARTEMIS_HOST" --pills_to_die="${#INDEXORS[@]}" --queue_port="$ARTEMIS_PORT"  --output_path="$(realpath "./output_${i}")"
# done


#Artemis
#"$ARTEMIS_PATH/create_artemis.sh" "$ARTEMIS_PATH" "$ARTEMIS_HOST"  "$ARTEMIS_PORT" "$HTTP_PORT" "$ARTEMIS_MEMORY_MAX"
# pkill -P $$

    






