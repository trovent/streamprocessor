#!/bin/bash

FOLDER="$1"

for consumer_file in $(ls "$FOLDER")
do
    echo importing consumer from $consumer_file
    curl --silent -XPOST --header 'Content-type: application/json' --data  @"$FOLDER/${consumer_file}"  http://localhost:8080/api/kafka/consumer && echo
done