#!/bin/bash

FOLDER="$1"

for producer_file in $(ls "$FOLDER")
do
    echo importing producer from $producer_file
    curl --silent -XPOST --header 'Content-type: application/json' --data  @"$FOLDER/${producer_file}"  http://localhost:8080/api/kafka/producer && echo
done