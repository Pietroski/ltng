#!/usr/bin/env bash

# script echoes a default DB_SOURCE_URL - playground one

DB_DRIVER=$(echo $DB_DRIVER)
DB_USERNAME=$(echo $DB_USER)
DB_PASSWORD=$(echo $DB_PASSWORD)
DB_HOST=$(echo $DB_HOST)
DB_PORT=$(echo $DB_PORT)
DB_NAME=$(echo $DB_NAME)
DB_SSL_MODE=$(echo $DB_SSL_MODE)
DB_SOURCE_URL="$DB_DRIVER://$DB_USERNAME:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME?sslmode=$DB_SSL_MODE" # param1=true&param2=false

echo "$DB_SOURCE_URL"
