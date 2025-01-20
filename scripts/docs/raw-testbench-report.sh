#!/bin/bash

# Get the current timestamp and date
TIMESTAMP=$(date +'%y-%m-%d/%H:%M:%S')
DATEDIR=$(date +'%y-%m-%d')

# Create output directory
mkdir -p "./docs/outputs/$DATEDIR"

# Create file with client results header
printf "CLIENT RESULTS\n\n" > "./docs/outputs/$TIMESTAMP.txt"

# Run client tests
timeout 5s bash -c 'go clean -testcache && go test -v -race -run=TestClients ./tests/integration/...' \
    >> "./docs/outputs/$TIMESTAMP.txt" || true

# Add engine results header
printf "\n\nENGINE RESULTS\n\n" >> "./docs/outputs/$TIMESTAMP.txt"

# Run engine benchmarks
timeout 5s bash -c 'go clean -testcache && go test -v -race -run=BenchmarkAllEngines -bench=BenchmarkAllEngines ./tests/benchmark/...' \
    >> "./docs/outputs/$TIMESTAMP.txt" || true
