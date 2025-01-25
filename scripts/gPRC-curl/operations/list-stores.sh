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

RESPONSE=$(grpcurl -d "$TRIMMED_STRING" \
    -plaintext localhost:50050 ltngdb.LightningDB/ListStores)

function decoder() {
    echo "$1" | jq -r '.dbsInfos'
}

decoder "$RESPONSE"
