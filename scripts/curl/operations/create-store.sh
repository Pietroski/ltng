#!/usr/bin/env bash

PAYLOAD='{
    "name": "postman-test-db",
    "path": "postman/test_db"
}'

TRIMMED_STRING=$(echo "$PAYLOAD" | tr -d '\n' | tr -d ' ')

curl -X POST \
    http://localhost:7070/ltng_db/v1/create_store \
    -H "Content-Type: application/json" \
    -d "$TRIMMED_STRING"
