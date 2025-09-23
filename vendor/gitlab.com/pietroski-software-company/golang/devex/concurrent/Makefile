# General scripts for the application

-include ./scripts/docs/Makefile

# Drone-ci makefile commands
-include .pipelines/.drone/Makefile

## generates mocks
mocks:
	go get github.com/maxbrunsfeld/counterfeiter/v6
	go get go.uber.org/mock/mockgen
	go mod vendor
	go generate ./...
	go mod tidy
	go mod vendor

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
