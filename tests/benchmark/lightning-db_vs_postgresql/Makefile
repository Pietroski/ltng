# Makefile

#include ./tests/benchmark/lightning-db_vs_postgresql/.env.benchmark.ltng.db.vs.psql
#export

## check envs from environment
benchmark-env-check-ltng-db-vs-psql:
	@echo ${POSTGRESQL_DATA_SOURCE_NAME}
	@echo ${LTNG_MANAGER_ADDRESS}

## installs go-migrate binary
benchmark-get-migrator:
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
	go mod tidy
	go mod vendor

benchmark-migrate-init-all:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/init/run.sh

benchmark-migrate-init-for:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/init/run-for.sh ${DOMAIN} ${SCHEMA_NAME}

benchmark-migrate:
	@./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/migrate.sh ${DOMAIN_NAME} ${DB_ENV_NAME} ${TYPE} ${OFFSET}

benchmark-migrate-all-up:
	@./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/up/migrate-all-up.sh ${DB_ENV_NAME}

benchmark-migrate-all-down:
	@./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/down/migrate-all-down.sh

## creates sqlc config file(s)
benchmark-sqlc-init:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/codegen/sqlc/sqlc-init.sh

## generates sqlc go code from sql files
benchmark-sqlc-gen:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/codegen/sqlc/sqlc-generate.sh
	go mod tidy
	go mod vendor

benchmark-docker-compose-up:
	docker compose -f ./tests/benchmark/lightning-db_vs_postgresql/build/orquestrator/docker-compose.yml up -d

benchmark-docker-compose-up-no-cache:
	docker compose -f ./tests/benchmark/lightning-db_vs_postgresql/build/orquestrator/docker-compose.yml up --build -d

benchmark-docker-compose-down:
	docker compose -f ./tests/benchmark/lightning-db_vs_postgresql/build/orquestrator/docker-compose.yml down
