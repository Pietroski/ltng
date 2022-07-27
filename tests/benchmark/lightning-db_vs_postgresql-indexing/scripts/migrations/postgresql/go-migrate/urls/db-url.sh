#!/usr/bin/env bash

PREFIX_DIR="tests/benchmark/lightning-db_vs_postgresql"
BASE_DATASTORE_PATH="$PREFIX_DIR/scripts/migrations/postgresql/go-migrate/urls"

ENVIRONMENT_NAME=${1?"A migration domain name should be provided!"}

declare -A DB_URLS=(
    ["psql-benchmark"]="$BASE_DATASTORE_PATH/psql-benchmark-db-url.sh"

    ["example"]="$BASE_DATASTORE_PATH/example-src-url.sh"
)

${DB_URLS[$ENVIRONMENT_NAME]}
