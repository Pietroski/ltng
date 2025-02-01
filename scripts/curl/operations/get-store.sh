#!/usr/bin/env bash

PAYLOAD='{
   "name": "postman-test-db"
}'

TRIMMED_STRING=$(echo "$PAYLOAD" | tr -d '\n' | tr -d ' ')

RESPONSE=$(curl -X POST \
 http://localhost:7070/ltng_db/v1/get_store \
 -H "Content-Type: application/json" \
 -d "$TRIMMED_STRING")

echo "$RESPONSE" | jq -r '.dbInfo'
