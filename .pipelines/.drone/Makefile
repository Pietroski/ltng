# Drone docker commands list

DOCKER_TAG := $(shell cat .pipelines/.drone/DOCKER_VERSION)

drone-clean-validate:
	docker image build --no-cache --progress=plain -t pietroski/drone-jsonnet-validate:"${DOCKER_TAG}" -f .pipelines/.drone/validate.Dockerfile .

build-drone-validate:
	docker image build --no-cache -t pietroski/drone-jsonnet-validate:"${DOCKER_TAG}" -f .pipelines/.drone/validate.Dockerfile . # --progress=plain (after build)

run-drone-validate:
	docker run --name drone-validate pietroski/drone-jsonnet-validate:"${DOCKER_TAG}" # --rm
	docker cp drone-validate:drone-analysis .pipelines/.drone/reports/drone-analysis
	docker cp drone-validate:drone-analysis.json .pipelines/.drone/reports/drone-analysis.json
	docker stop drone-validate
	docker container rm drone-validate

rm-drone-validate:
	docker container stop drone-validate
	docker container rm drone-validate

drone-validate: build-drone-validate run-drone-validate
	@echo "done validating drone jsonnet file"

# Validate Docker-in-Docker functionality
validate-docker-in-docker:
	@echo "Validating Docker-in-Docker functionality..."
	@cd $(PWD) && bash .pipelines/.drone/validate-docker-in-docker.sh

# Run Docker-in-Docker validation in a container (simulates drone environment)
validate-docker-in-docker-container:
	@echo "Running Docker-in-Docker validation in container..."
	docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
		-v $(PWD):/workspace \
		-w /workspace \
		--privileged \
		pietroski/alpine-docker-golang:v0.0.8 \
		./.pipelines/.drone/validate-docker-in-docker.sh
