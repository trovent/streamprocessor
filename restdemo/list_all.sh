#!/bin/bash

echo "getting all schemas:"
curl --silent -XGET http://localhost:8080/api/schemas |jq

echo "getting all statements:"
curl --silent -XGET http://localhost:8080/api/statements |jq

echo "getting all consumers:"
curl --silent -XGET http://localhost:8080/api/consumers |jq

echo "getting all producers:"
curl --silent -XGET http://localhost:8080/api/producers |jq
