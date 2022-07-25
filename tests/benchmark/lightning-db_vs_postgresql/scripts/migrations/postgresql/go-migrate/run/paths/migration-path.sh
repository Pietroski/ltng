#!/usr/bin/env bash

PREFIX_DIR="tests/benchmark/lightning-db_vs_postgresql"
BASE_DATASTORE_PATH="$PREFIX_DIR/internal/adaptors/datastore/postgresql"
MIGRATIONS="migrations"

USER="user"

MIGRATION_NAME=${1?"A migration domain name should be provided!"}

declare -A migrations=(
    # comment if not needed
    ["user"]="$BASE_DATASTORE_PATH/$USER/$MIGRATIONS"
)

echo "${migrations[$MIGRATION_NAME]}"
