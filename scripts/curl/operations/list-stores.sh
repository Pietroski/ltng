#!/usr/bin/env bash

function ToBase64() {
    echo "$1" | base64
}

function stringify() {
    echo "$1" | jq -R .
}

function trimmer() {
    echo "$1" | tr -d '\n' | tr -d ' '
}

PAYLOAD='
{
  "pagination": {
    "page_id": 1,
    "page_size": 10
  }
}
'
#echo "$PAYLOAD"

TRIMMED_STRING=$(trimmer "$PAYLOAD")
echo "$TRIMMED_STRING"

RESPONSE=$(curl -X POST \
    http://localhost:8080/ltng_db/v1/list_stores \
    -H "Content-Type: application/json" \
    -d "$TRIMMED_STRING")

echo "$RESPONSE" | jq -r '.dbsInfos'
