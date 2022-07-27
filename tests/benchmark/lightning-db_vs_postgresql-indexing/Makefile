#

#include ./tests/benchmark/lightning-db_vs_postgresql/.env.benchmark.ltng.db.vs.psql
#export

## check envs from environment
env-check-ltng-db-vs-psql-indexing:
	@echo ${POSTGRESQL_DATA_SOURCE_NAME}
	@echo ${LTNG_MANAGER_ADDRESS}

## installs go-migrate binary
get-migrator-indexing:
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
	go mod tidy
	go mod vendor

migrate-init-all-indexing:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/init/run.sh

migrate-init-for-indexing:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/init/run-for.sh ${DOMAIN} ${SCHEMA_NAME}

migrate-indexing:
	@./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/migrate.sh ${DOMAIN_NAME} ${DB_ENV_NAME} ${TYPE} ${OFFSET}

migrate-all-up-indexing:
	@./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/up/migrate-all-up.sh ${DB_ENV_NAME}

migrate-all-down-indexing:
	@./tests/benchmark/lightning-db_vs_postgresql/scripts/migrations/postgresql/go-migrate/run/types/down/migrate-all-down.sh

## creates sqlc config file(s)
sqlc-init-indexing:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/codegen/sqlc/sqlc-init.sh

## generates sqlc go code from sql files
sqlc-gen-indexing:
	./tests/benchmark/lightning-db_vs_postgresql/scripts/codegen/sqlc/sqlc-generate.sh
	go mod tidy
	go mod vendor

docker-compose-up-indexing:
	docker compose -f ./tests/benchmark/lightning-db_vs_postgresql/build/orquestrator/docker-compose.yml up -d

docker-compose-down-indexing:
	docker compose -f ./tests/benchmark/lightning-db_vs_postgresql/build/orquestrator/docker-compose.yml down

#run-benchmark-ltng-db-vs-psql:
#
