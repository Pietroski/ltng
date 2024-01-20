# lightning-db-node's build related Makefile command list

LTNG_DB_VERSION := $(shell cat ./build/docker/DOCKER_VERSION)

## lightning-db-node's version
ltng-db-node-version:
	@echo ${LTNG_DB_VERSION}

########################################################################################################################

## docker lightning-db-node's image build
build-ltng-db-node-image:
	docker image build -t pietroski/lightning-db-node -f build/docker/lightning-db-node.Dockerfile . # ../../../

## docker lightning-db-node's image build with no caching
build-ltng-db-node-image-no-cache:
	docker image build --progress=plain --no-cache -t pietroski/lightning-db-node -f build/docker/lightning-db-node.Dockerfile .

## docker lightning-db's image tag
tagging-ltng-db-node-image:
	docker tag pietroski/lightning-db-node pietroski/lightning-db-node:${LTNG_DB_VERSION}

## docker lightning-db-node's image push update
update-docker-ltng-db-node-image:
	docker push pietroski/lightning-db-node:${LTNG_DB_VERSION}

## docker builds, tags and pushes lightning-db-node's image to docker docker hub
ltng-docker-patch: build-ltng-db-node-image tagging-ltng-db-node-image update-docker-ltng-db-node-image

#########################################################################################################################

start-local-ltng-db:
	DOCKER_BUILDKIT=0 docker compose -f ./build/orchestrator/docker-compose.yml up -d --build

stop-local-ltng-db:
	docker compose -f ./build/orchestrator/docker-compose.yml down

#########################################################################################################################
#
#remote-build-ltng-db-node-image:
#	build/docker/scripts/remote-build-ltng-db-node-image.sh $(REMOTE) $(API_REMOTE) $(NETRC_LOGIN) $(NETRC_PASSWORD)
#
### remote docker lightning-db-node's image build
#remote-build-ltng-db-node-image:
#	docker image build -t pietroski/lightning-db-node -f build/docker/remote/lightning-db-node.Dockerfile . # ../../../
#
### remote docker lightning-db-node's image build with no caching
#remote-build-ltng-db-node-image-no-cache:
#	docker image build --no-cache -t pietroski/lightning-db-node -f build/docker/remote/lightning-db-node.Dockerfile .
#
### docker builds, tags and pushes lightning-db-node's image to docker docker hub
#remote-ltng-docker-patch: remote-build-ltng-db-node-image tagging-ltng-db-node-image update-docker-ltng-db-node-image
#
#########################################################################################################################
