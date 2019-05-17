#!/bin/bash

DEFAULT_DATA=' { "name" : "Kirk", "age" :42 , "isEmployed" : true, "rating" : 0.7  } '
DATA=${1:-$DEFAULT_DATA}

CONTAINER="streamprocessor_kafka_1"

TOPIC="input"

BROKER="kafka:9092"

echo "$DATA" | docker exec -i $CONTAINER  kafka-console-producer.sh --topic $TOPIC --broker-list $BROKER
