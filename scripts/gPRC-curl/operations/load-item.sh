#!/usr/bin/env bash

function ToBase64() {
    # shellcheck disable=SC2005
    echo "$(echo "$1" | base64)"
}

INDEX_01="test-key"

BASE64_INDEX_01=$(ToBase64 "$INDEX_01")

PAYLOAD=$(
    cat <<EOF
{
    "database_meta_info": {
        "database_name": "postman-test-db"
    },
    "item": {
        "key": "$BASE64_INDEX_01"
    }
}
EOF
)

# shellcheck disable=SC2120
function stringify() {
    echo "$1" | jq -R .
}

function trimmer() {
    echo "$1" | tr -d '\n' | tr -d ' '
}

TRIMMED_STRING=$(trimmer "$PAYLOAD")
echo "$TRIMMED_STRING"

RESPONSE=$(grpcurl -d "$TRIMMED_STRING" \
    -plaintext localhost:50052 operations.Operation/Load)

function decoder() {
    echo "$1" | jq -r '.value' | base64 --decode | jq .
}

decoder "$RESPONSE"
