#!/usr/bin/env bash

# RELATIVE_PATH_FROM_ROOT=../../../.. # => if called from the file
RELATIVE_PATH_FROM_ROOT=. # if called from the Makefile

DOMAIN=${1?"A migration domain name should me provided"}
SCHEMA_NAME=${2?"A schema name should be provided by quotes"}

PATHS="${RELATIVE_PATH_FROM_ROOT}/tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/paths/migration-path.sh"
DESTINATION=$($PATHS "$DOMAIN")

migrate create -ext sql -dir "$DESTINATION" -seq "$SCHEMA_NAME"
