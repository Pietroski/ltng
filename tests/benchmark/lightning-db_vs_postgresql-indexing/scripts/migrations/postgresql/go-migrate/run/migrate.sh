#!/usr/bin/env bash

DOMAIN_NAME=${1?"A DOMAIN_NAME should be provided!"}
DB_ENV_NAME=${2?"A DB_ENV_NAME should be provided!"}
TYPE=$3
OFFSET=$4

PREFIX_DIR="tests/benchmark/lightning-db_vs_postgresql"
# calls from the Makefile
MIGRATION_PATH=$(./"$PREFIX_DIR"/scripts/migrations/postgresql/go-migrate/run/paths/migration-path.sh "$DOMAIN_NAME")
DB_SOURCE_URL=$(./"$PREFIX_DIR"/scripts/migrations/postgresql/go-migrate/urls/db-url.sh "$DB_ENV_NAME")
# verify whether not empty
if [[ -z $MIGRATION_PATH ]]; then
    echo "invalid MIGRATION_PATH key"
    exit 1
elif [[ -z $DB_SOURCE_URL ]]; then
    echo "invalid DB_SOURCE_URL key"
    exit 1
fi

MIGRATE=migrate
# shellcheck disable=SC2206
declare -a MIGRATE_BASE_COMMAND=(${MIGRATE} -path ${MIGRATION_PATH} -database ${DB_SOURCE_URL} -verbose)

echo "Migrations -=> $TYPE -- $OFFSET || " "${MIGRATE_BASE_COMMAND[@]}"

case "$TYPE" in
'init')
    echo "Versioning migrations++"
    ${MIGRATE} create -ext sql -dir "${MIGRATION_PATH}" -seq init_schema
    ;;
'up')
    if [[ "$OFFSET" -gt 0 ]]; then
        echo "Migrating up with offset..."
        "${MIGRATE_BASE_COMMAND[@]}" up "${OFFSET}"
    else
        echo "Migrating up..."
        "${MIGRATE_BASE_COMMAND[@]}" up
    fi
    ;;
'down')
    if [[ "$OFFSET" -gt 0 ]]; then
        echo "Migrating down with offset..."
        "${MIGRATE_BASE_COMMAND[@]}" down "${OFFSET}"
    else
        echo "Migrating down..."
        "${MIGRATE_BASE_COMMAND[@]}" down
    fi
    ;;
esac
