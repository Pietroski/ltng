# Makefile command list

# Makefile Documentation parser
-include ./scripts/docs/Makefile

# Drone's pipeline validation
-include .pipelines/.drone/Makefile

########################################################################################################################

## generates mocks
mocks:
	go get github.com/maxbrunsfeld/counterfeiter/v6
	go get go.uber.org/mock/mockgen
	go mod vendor
	go generate ./...
	go mod tidy
	go mod vendor

test-unit:
	go test -race -v `go list ./... | grep -v ./pkg/mocks`

test-unit-cover:
	go test -race -v -coverprofile ./docs/reports/tests/unit/cover.out `go list ./... | grep -v ./pkg/mocks`

test-unit-cover-silent:
	go test -race -coverprofile ./docs/reports/tests/unit/cover.out `go list ./... | grep -v ./pkg/mocks`

test-unit-cover-all:
	go test -race -v -coverprofile ./docs/reports/tests/unit/cover-all.out ./...

test-unit-cover-all-silent:
	go test -race -coverprofile ./docs/reports/tests/unit/cover-all.out ./...

test-unit-cover-report:
	go tool cover -html=./docs/reports/tests/unit/cover.out

test-unit-cover-all-report:
	go tool cover -html=./docs/reports/tests/unit/cover-all.out

########################################################################################################################

gen-goroutine-profile:
	go tool pprof --pdf ./profiling/pprof/goroutine.pprof > ./docs/reports/profiling/pprof/goroutine-profiling.pdf

gen-threadcreation-profile:
	go tool pprof --pdf ./profiling/pprof/threadcreation.pprof > ./docs/reports/profiling/pprof/threadcreation-profiling.pdf

profiling: gen-goroutine-profile gen-threadcreation-profile

run-main-test:
	go run -race cmd/test/handler-leak-tracing/manual-stopping/main.go

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

commit-changelog:
	git add .
	git commit -m "chore: changelog"

gitea-push-main:
	git push gitea main

gitlab-push-main:
	git push gitlab main

push-main-all: gitea-push-main gitlab-push-main

amend:
	git commit --amend

rebase-continue:
	git rebase --continue

trigger-pipeline:
	git commit --amend
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

########################################################################################################################

count-written-lines:
	./scripts/metrics/line-counter

########################################################################################################################
