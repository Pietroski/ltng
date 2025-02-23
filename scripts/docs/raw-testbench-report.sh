#!/bin/bash

# Get the current timestamp and date
TIMESTAMP=$(date +'%y-%m-%d/%H:%M:%S')
DATEDIR=$(date +'%y-%m-%d')

# Create output directory
mkdir -p "./docs/outputs/$DATEDIR"

printf "|TESTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

printf "\n\n|RACE_TESTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Add engine results header
printf "\n\n|ENGINE_RESULTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Run engine tests
timeout 50s bash -c 'go clean -testcache && go test -v -race -run=TestEngines ./tests/integration/...' \
    >>"./docs/outputs/$TIMESTAMP.txt" || true

# Create file with client results header
printf "\n\n|LOCAL_CLIENT_RESULTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Run client tests
timeout 50s bash -c 'go clean -testcache && go test -v -race -run=TestClientsLocally ./tests/integration/...' \
    >>"./docs/outputs/$TIMESTAMP.txt" || true

# Create file with client results header
printf "\n\n|DOCKER_CLIENT_RESULTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Run client tests
timeout 50s bash -c 'go clean -testcache && go test -v -race -run=TestClientsWithinDocker ./tests/integration/...' \
    >>"./docs/outputs/$TIMESTAMP.txt" || true

printf "\n\n|NO_RACE_TESTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Add engine results header
printf "\n\n|ENGINE RESULTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Run engine tests
timeout 50s bash -c 'go clean -testcache && go test -v -run=TestEngines ./tests/integration/...' \
    >>"./docs/outputs/$TIMESTAMP.txt" || true

# Create file with client results header
printf "\n\n|LOCAL_CLIENT_RESULTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Run client tests
timeout 50s bash -c 'go clean -testcache && go test -v -run=TestClientsLocally ./tests/integration/...' \
    >>"./docs/outputs/$TIMESTAMP.txt" || true

# Create file with client results header
printf "\n\n|DOCKER_CLIENT_RESULTS|\n\n" >>"./docs/outputs/$TIMESTAMP.txt"

# Run client tests
timeout 50s bash -c 'go clean -testcache && go test -v -run=TestClientsWithinDocker ./tests/integration/...' \
    >>"./docs/outputs/$TIMESTAMP.txt" || true
