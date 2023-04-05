# General scripts for the application

-include ./scripts/docs/Makefile
-include ./scripts/schemas/Makefile

# Drone-ci makefile commands
-include .pipelines/.drone/Makefile

-include ./build/Makefile

-include ./tests/benchmark/lightning-db_vs_postgresql/Makefile
-include ./tests/benchmark/lightning-db_vs_postgresql-indexing/Makefile

-include ./tests/integration/lightning-db/Makefile

#include .env
#export

export-envs:
	@export $(xargs <./.env)

## check envs from environment
env-check-ltng-db-node:
	@echo ${LTNG_MANAGER_NETWORK}
	@echo ${LTNG_MANAGER_ADDRESS}

## generates mocks
mock-generate:
	go get -d github.com/golang/mock/mockgen
	go mod vendor
	go generate ./...
	go mod tidy
	go mod vendor

go-build:
	@go build -ldflags="-w -s" -o cmd/badgerdb/grpc/lightning-db-node cmd/badgerdb/grpc/main.go

run: export-envs
	@go run cmd/badgerdb/grpc/main.go

TYPE:=goroutine
pprof-serve:
	@go tool pprof -http=":7002" "http://localhost:7001/debug/pprof/$(TYPE)"

########################################################################################################################

count-written-lines:
	find . -type f \( -iname "*.go" ! -ipath "./vendor/*" ! -path "./schemas/*" ! -path "*/postgresql/*" ! -path "*/mocks/*" ! -path ".pipelines/*" \) | xargs wc -l

TAG := $(shell cat VERSION)
tag:
	git tag $(TAG)
