#!/bin/bash

FOLDER="$1"

for stmt_file in $(ls "$FOLDER")
do
    echo importing statement from $stmt_file
    curl --silent -XPOST --header 'Content-type: application/json' --data  @"$FOLDER/${stmt_file}"  http://localhost:8080/api/esper/statement && echo
done