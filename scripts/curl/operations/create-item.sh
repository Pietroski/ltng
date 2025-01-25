#!/usr/bin/env bash

function ToBase64() {
    echo "$1" | base64
}

function ToBytes() {
    echo -n "$1" | xxd -p | sed 's/\(..\)/\\x\1/g'
}

# shellcheck disable=SC2120
function stringify() {
    echo "$1" | jq -R .
}

function trimmer() {
    echo "$1" | tr -d '\n' | tr -d ' '
}

VALUE='
{
    "test-key": "test-value",
    "test-index": "test-index-value",
    "another-test-index": "another-test-index-value",
    "just-another-field": "just-another-field-value"
}
'

BASE64_VALUE=$(ToBase64 "$VALUE")
INDEX_01="test-key"
INDEX_02="test-index"
INDEX_03="another-test-index"

BASE64_INDEX_01=$(ToBase64 "$INDEX_01")
BASE64_INDEX_02=$(ToBase64 "$INDEX_02")
BASE64_INDEX_03=$(ToBase64 "$INDEX_03")

KEY_LIST='["'$BASE64_INDEX_01'", "'$BASE64_INDEX_02'", "'$BASE64_INDEX_03'"]'

PAYLOAD='{
    "database_meta_info": {
        "database_name": "postman-test-db"
    },
    "item": {
        "key": "'$BASE64_INDEX_01'",
        "value": "'$BASE64_VALUE'"
    },
    "index_opts": {
        "has_idx": true,
        "parent_key": "'$BASE64_INDEX_01'",
        "indexing_keys": '$KEY_LIST'
    }
}'

TRIMMED_STRING=$(trimmer "$PAYLOAD")

curl -X POST \
    http://localhost:8080/ltng_db/v1/create_item \
    -H "Content-Type: application/json" \
    -d "$TRIMMED_STRING"
