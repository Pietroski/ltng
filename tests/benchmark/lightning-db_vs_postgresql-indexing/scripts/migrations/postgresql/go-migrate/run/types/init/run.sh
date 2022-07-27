#!/usr/bin/env bash

# RELATIVE_PATH_FROM_ROOT=../../../.. # => if called from the file
RELATIVE_PATH_FROM_ROOT=. # if called from the Makefile

RUN_FOR=${RELATIVE_PATH_FROM_ROOT}/tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/init/run-for.sh
MIGRATION_SCHEMA_DEFAULT_NAME="migrating_again"

declare -a DOMAINS=(
    "user"
)

for domain in "${DOMAINS[@]}"; do
    "$RUN_FOR" "$domain" "$MIGRATION_SCHEMA_DEFAULT_NAME"
done
