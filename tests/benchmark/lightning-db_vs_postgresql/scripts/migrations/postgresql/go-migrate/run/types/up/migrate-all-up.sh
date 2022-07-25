#!/usr/bin/env bash

PREFIX_DIR="tests/benchmark/lightning-db_vs_postgresql"
MIGRATOR_PATH="./$PREFIX_DIR/scripts/migrations/postgresql/go-migrate/run/migrate.sh"

declare -a DOMAINS=(
    "user"
)

DB_ENV_NAME=$1
if [[ -z $MIGRATION_PATH ]]; then
    echo "DB_ENV_NAME is empty - setting default to psql-benchmark"
    DB_ENV_NAME="psql-benchmark"
fi

for domain in "${DOMAINS[@]}"; do
    # call the migration script here
    echo "Domain migration => $domain"
    $MIGRATOR_PATH "$domain" $DB_ENV_NAME up
done
