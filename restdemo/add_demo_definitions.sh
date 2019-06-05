#!/bin/bash

SCHEMA_A='{ "name" : "Person", "fields" : { "name" : "string", "age" : "integer", "isEmployed" : "boolean", "rating" : "float" } }'
STMT_A='{ "name" : "AdultFilter", "expression" : "select * from Person where age>=18" }'


CONSUMER='{ "topic" : "input", "schemaName" : "Person" }'
PRODUCER='{ "topic" : "output", "eplStatementName" : "AdultFilter" }'

echo "adding schema: ${SCHEMA_A}"
curl --silent -XPOST --header 'Content-type: application/json' --data  "${SCHEMA_A}"  http://localhost:8080/api/esper/schema && echo

echo "adding statement: ${STMT_A}"
curl --silent -XPOST --header 'Content-type: application/json' --data  "${STMT_A}"  http://localhost:8080/api/esper/statement && echo

echo "adding consumer connector: ${CONSUMER}"
curl --silent -XPOST --header 'Content-type: application/json' --data  "${CONSUMER}"  http://localhost:8080/api/kafka/consumer && echo

echo "adding producer connector: ${PRODUCER}"
curl --silent -XPOST --header 'Content-type: application/json' --data  "${PRODUCER}"  http://localhost:8080/api/kafka/producer && echo

