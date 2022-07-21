#!/bin/bash
# Currently we only support running tests
INDEXORS=('idnes.cz', 'aktualne.cz', 'novinky.cz', 'seznamzpravy.cz', 'irozhlas.cz')
ARTEMIS_HOST="localhost"
AGGREGATOR_EXEC="./Aggregator/aggregate.sh"
PROCESSOR_EXEC="./Processor/process.sh"
ARTEMIS_EXEC="./Artemis/create_artemis.sh"
PROCESSORS_NUM=64

#Artemis

qsub -cwd -j y "$ARTEMIS_EXEC" $ARTEMIS_HOST

# Aggregators
for index in "${INDEXORS[@]}"; do
    qsub -cwd -j y "$AGGREGATOR_EXEC" --queue_host="$ARTEMIS_HOST" "$index"
done


#TODO pills to die
#Processors
for i in $PROCESSORS_NUM; do
    qsub -cwd -j y "$PROCESSOR_EXEC" --queue_host="$ARTEMIS_HOST" "article_${i}"
done


    






