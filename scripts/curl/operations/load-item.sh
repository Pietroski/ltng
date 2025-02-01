#!/usr/bin/env bash

KEY="test-key"
BASE64_KEY=$(echo "$KEY" | base64)

PAYLOAD='{
   "database_meta_info": {
       "database_name": "postman-test-db"
   },
   "item": {
       "key": "'$BASE64_KEY'"
   }
}'

TRIMMED_STRING=$(echo "$PAYLOAD" | tr -d '\n' | tr -d ' ')

RESPONSE=$(curl -X POST \
 http://localhost:7070/ltng_db/v1/load_item \
 -H "Content-Type: application/json" \
 -d "$TRIMMED_STRING")

echo "$RESPONSE" | jq -r '.value' | base64 --decode | jq .
