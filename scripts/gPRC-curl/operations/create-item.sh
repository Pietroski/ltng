#!/usr/bin/env bash

function ToBase64() {
    # shellcheck disable=SC2005
    echo "$(echo "$1" | base64)"
}

VALUE=$(
    cat <<EOF
{
    "test-key": "test-value",
    "test-index": "test-index-value",
    "another-test-index": "another-test-index-value",
    "just-another-field": "just-another-field-value"
}
EOF
)

BASE64_VALUE=$(ToBase64 "$VALUE")

#echo "$BASE64_VALUE"

INDEX_01="test-key"
INDEX_02="test-index"
INDEX_03="another-test-index"

BASE64_INDEX_01=$(ToBase64 "$INDEX_01")
BASE64_INDEX_02=$(ToBase64 "$INDEX_02")
BASE64_INDEX_03=$(ToBase64 "$INDEX_03")
#echo "$BASE64_INDEX_01"

KEY_LIST="[$BASE64_INDEX_01,$BASE64_INDEX_02,$BASE64_INDEX_03]"
BASE64_KEY_LIST=$(ToBase64 "$KEY_LIST")
#echo "$BASE64_KEY_LIST"

PAYLOAD=$(
    cat <<EOF
{
    "database_meta_info": {
        "database_name": "postman-test-db"
    },
    "item": {
        "key": "$BASE64_INDEX_01",
        "value": "$BASE64_VALUE"
    },
    "index_opts": {
        "has_idx": true,
        "parent_key": "$BASE64_INDEX_01",
        "indexing_keys": "$BASE64_KEY_LIST"
    }
}
EOF
)
#echo "$PAYLOAD"

# shellcheck disable=SC2120
function stringify() {
    echo "$1" | jq -R .
}

function trimmer() {
    echo "$1" | tr -d '\n' | tr -d ' '
}

TRIMMED_STRING=$(trimmer "$PAYLOAD")
echo "$TRIMMED_STRING"

grpcurl -d "$TRIMMED_STRING" \
    -plaintext localhost:50050 ltngdb.LightningDB/Create
