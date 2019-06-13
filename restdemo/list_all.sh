#!/bin/bash

echo "getting all schemas:"
curl --silent -XGET http://localhost:8080/api/esper/schemas |jq

echo "getting all statements:"
curl --silent -XGET http://localhost:8080/api/esper/statements |jq

echo "getting all consumers:"
curl --silent -XGET http://localhost:8080/api/kafka/consumers |jq

echo "getting all producers:"
curl --silent -XGET http://localhost:8080/api/kafka/producers |jq
