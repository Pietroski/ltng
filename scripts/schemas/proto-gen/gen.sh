#!/usr/bin/env bash

MOD_NAME=gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated
PROTO_PATH=schemas/protos
OUTPUT_PATH=schemas/generated
OUTPUT_COMPILED_PATH=schemas/compiled
SWAGGER_PATH="$OUTPUT_PATH/swagger"

mkdir -p $OUTPUT_PATH
mkdir -p $OUTPUT_COMPILED_PATH

declare -a proto_list=(
    "common/queries/config"
    "common/search"

    "management"
    "transactions/operations"
)

declare -A proto_list_map=(
    ["common/queries/config"]="common_queries_config"
    ["common/search"]="common_search"

    ["management"]="management"
    ["transactions/operations"]="transactions_operations"
)

for domain in "${proto_list[@]}"; do
    protoc --proto_path="$PROTO_PATH" "$PROTO_PATH"/"${domain}"/*.proto \
        --go_out=:"$OUTPUT_PATH" \
        --go_opt=module="$MOD_NAME" \
        --go-grpc_out=:"$OUTPUT_PATH" \
        --go-grpc_opt=module="$MOD_NAME" \
        --grpc-gateway_out=:"$OUTPUT_PATH" \
        --grpc-gateway_opt=module="$MOD_NAME" \
        --openapiv2_out=:"$SWAGGER_PATH" \
        --descriptor_set_out="$OUTPUT_COMPILED_PATH"/"${proto_list_map[$domain]}".ltng_db.protoset \
        --include_imports
done

go mod tidy
go mod download
go mod vendor
