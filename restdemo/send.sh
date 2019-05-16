#!/bin/bash


DATA=' { "name" : "Kirk", "age" :42 , "isEmployed" : true, "rating" : 0.7  } '

CONTAINER="streamprocessor_kafka_1"

TOPIC="input"

BROKER="kafka:9092"

echo "$DATA" | docker exec -i $CONTAINER  kafka-console-producer.sh --topic $TOPIC --broker-list $BROKER
