#!/usr/bin/env bash

function ToBase64() {
    # shellcheck disable=SC2005
    echo "$(echo "$1" | base64)"
}

PAYLOAD=$(
    cat <<EOF
{
    "name": "postman-test-db",
    "path": "postman/test_db"
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
    -plaintext localhost:50050 ltngdb.LightningDB/CreateStore
