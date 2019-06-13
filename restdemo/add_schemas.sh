#!/bin/bash

FOLDER="$1"

for schema_file in $(ls "$FOLDER")
do
    echo importing schema from $schema_file
    curl --silent -XPOST --header 'Content-type: application/json' --data  @"$FOLDER/${schema_file}"  http://localhost:8080/api/esper/schema && echo
done