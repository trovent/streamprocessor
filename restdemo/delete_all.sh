#!/bin/bash

CONSUMER_IDS=$(curl --silent -XGET http://localhost:8080/api/kafka/consumers | jq -r -c .[].id )

for ID in $CONSUMER_IDS
do 
    echo deleting consumer $ID
    curl --silent -XDELETE http://localhost:8080/api/kafka/consumer/$ID
done


PRODUCER_IDS=$(curl --silent -XGET http://localhost:8080/api/kafka/producers | jq -r -c .[].id )

for ID in $PRODUCER_IDS
do 
    echo deleting producer $ID
    curl --silent -XDELETE http://localhost:8080/api/kafka/producer/$ID
done


STMT_NAMES=$(curl --silent -XGET http://localhost:8080/api/esper/statements | jq -r -c .[].name )

for STMT_NAME in $STMT_NAMES
do
    echo deleting $STMT_NAME
    curl --silent -XDELETE http://localhost:8080/api/esper/statement/$STMT_NAME
done


SCHEMA_NAMES=$(curl --silent -XGET http://localhost:8080/api/esper/schemas | jq -r -c .[].name )

for SCHEMA_NAME in $SCHEMA_NAMES
do
    echo deleting $SCHEMA_NAME
    curl --silent -XDELETE http://localhost:8080/api/esper/schema/$SCHEMA_NAME
done
