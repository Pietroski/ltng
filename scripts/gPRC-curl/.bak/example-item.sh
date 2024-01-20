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

#echo "$VALUE"

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

# shellcheck disable=SC2005
#echo "$(ToBase64 "$VALUE")"

#ToBase64 "$VALUE"

#grpcurl -plaintext 127.0.0.1:50052 -import-path ../../schemas/protos/transactions/operations/operation_service.proto -proto my-stuff.proto list
#
#grpcurl -import-path schemas/protos --proto transactions/operations/operation_service.proto list
#grpcurl -import-path schemas/protos --proto transactions/operations/operation_service.proto describe operations.Operation
#
#grpcurl -protoset schemas/compiled/transactions_operations.ltng_db.protoset list
#grpcurl -protoset schemas/compiled/transactions_operations.ltng_db.protoset describe operations.Operation
#grpcurl -protoset schemas/compiled/transactions_operations.ltng_db.protoset describe .operations.CreateRequest
#
#grpcurl -plaintext 127.0.0.1:50052 -protoset schemas/compiled/transactions_operations.ltng_db.protoset .operations.CreateRequest
#
#grpcurl -plaintext localhost:50051 list
#grpcurl -plaintext localhost:50052 list
#grpcurl -plaintext localhost:50052 describe operations.Operation
#grpcurl -plaintext localhost:50052 describe .operations.CreateRequest

#grpcurl -d "$PAYLOAD" -plaintext localhost:50052 operations.Operation/Create
#echo "$PAYLOAD" | jq -R .

# shellcheck disable=SC2120
function stringify() {
    echo "$1" | jq -R .
}

#STRINGIFIED_PAYLOAD=$(stringify "$PAYLOAD")
#echo "$STRINGIFIED_PAYLOAD"

TRIMMED_STRING=$(echo "$PAYLOAD" | tr -d '\n' | tr -d ' ')
echo "$TRIMMED_STRING"

#STRINGIFIED_PAYLOAD=$(stringify "$TRIMMED_STRING")
#echo "$STRINGIFIED_PAYLOAD"

grpcurl -d "$TRIMMED_STRING" \
    -plaintext localhost:50052 operations.Operation/Create

########################################################################################################################

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

function trimmer() {
    echo "$1" | tr -d '\n' | tr -d ' '
}

TRIMMED_STRING=$(trimmer "$PAYLOAD")
echo "$TRIMMED_STRING"

RESPONSE=$(grpcurl -d "$TRIMMED_STRING" \
    -plaintext localhost:50052 operations.Operation/Load)

echo "$RESPONSE"
