# General scripts for the application

-include ./scripts/docs/Makefile
-include ./scripts/schemas/Makefile
-include ./scripts/gPRC-curl/grpc-curl.mk

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
	go get go.uber.org/mock/mockgen
	go mod vendor
	go generate ./...
	go mod tidy
	go mod vendor

go-build:
	@go build -ldflags="-w -s" -o cmd/badgerdb/grpc/lightning-db-node cmd/badgerdb/grpc/main.go

run: export-envs
	@go run cmd/badgerdb/grpc/main.go

full-local-test:
	DOCKER_BUILDKIT=0 make docker-compose-up-tests-integration-ltng-db
	go clean -testcache
	export $(grep -v '^#' ./tests/integration/lightning-db/.tests.integration.ltng.db.env | xargs)
	go test -race $(go list ./... | grep -v /tests/ | grep -v /mocks/ | grep -v /schemas/ | grep -v /benchmark/)
	make docker-compose-down-tests-integration-ltng-db

TYPE:=goroutine
pprof-serve:
	@go tool pprof -http=":7002" "http://localhost:7001/debug/pprof/$(TYPE)"

########################################################################################################################

pull-latest:
	git pull gitea main

add-all:
	git add .

commit-with:
	git commit -m "$${m}"

chore-version-bump:
	git add .
	git commit -m "chore: version bump"

TAG := $(shell cat VERSION)
tag:
	git tag $(TAG)

changelog:
	@./scripts/docs/changelog.sh

commit-changelog: add-all
	git commit -m "chore: changelog"

gitea-push-main:
	git push gitea main

gitlab-push-main:
	git push gitlab main

push-main-all: gitea-push-main gitlab-push-main

amend:
	git commit --amend --no-edit

rebase-continue:
	git rebase --continue

trigger-pipeline: amend
	git push gitea main --force-with-lease

gitea-push-tags:
	git push gitea --tags

gitlab-push-tags:
	git push gitlab --tags

push-tags: gitea-push-tags gitlab-push-tags

publish:
	make chore-version-bump
	make tag
	make changelog
	make commit-changelog
	make changelog
	make commit-changelog

clean-mod-cache:
	go clean -cache
	go clean -modcache
	go clean -testcache

########################################################################################################################

count-written-lines:
	./scripts/metrics/line-counter

########################################################################################################################
