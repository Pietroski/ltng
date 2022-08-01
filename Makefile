# General scripts for the application

include .env
export

export-envs:
	@export $(xargs <./.env)

## check envs from environment
env-check-ltng-db-node:
	@echo ${LTNG_MANAGER_NETWORK}
	@echo ${LTNG_MANAGER_ADDRESS}

-include ./scripts/docs/Makefile
-include ./scripts/schemas/Makefile

-include ./build/Makefile

-include ./tests/benchmark/lightning-db_vs_postgresql/Makefile
-include ./tests/benchmark/lightning-db_vs_postgresql-indexing/Makefile

## generates mocks
mock-generate:
	go get -d github.com/golang/mock/mockgen
	go mod download
	go generate internal/...
	go mod tidy
	go mod download

go-build:
	@go build -ldflags="-w -s" -o cmd/badgerdb/grpc/lightning-db-node cmd/badgerdb/grpc/main.go

run: export-envs
	@go run cmd/badgerdb/grpc/main.go

TYPE:=goroutine
pprof-serve:
	@go tool pprof -http=":7002" "http://localhost:7001/debug/pprof/$(TYPE)"

########################################################################################################################

count-written-lines:
	find . -type f \( -iname "*.go" ! -ipath "./vendor/*" ! -path "./schemas/*" ! -path "*/postgresql/*" ! -path "*/mocks/*" \) | xargs wc -l

TAG := $(shell cat VERSION)
tag:
	git tag $(TAG)
