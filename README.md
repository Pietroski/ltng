# Lightning-DB

Lightning-DB is not only a Badger-DB wrapper written in GoLang and that uses gRPC.
However, Lightning-DB is a graph database which has as its engine, Badger-DB and also uses gRPC as its transport layer.

LTNG_ENGINE=default badgerV3 badgerV4 ltng_engine

[//]: # (###############)

## Ltng-Eco

## Lightning - Ecosystem

- Lightning-DB
- Lightning-Cache
- Lightning-Queue
- Lightning-Storage

## Lightning-Queue

- can consume from:
  - live stream
  - historical stream

- a stream is created thought on how an event will be propagated (round-robin, fan out, etc...)

## Useful command list

```bash
docker compose -f build/orchestrator/docker-compose-test.yml up -d --build --remove-orphans
```

```bash
docker compose -f build/orchestrator/docker-compose-test.yml down
```

```bash
go test -v -race -run=TestClients ./tests/benchmark/...
```

```bash
go test -v -race -run=BenchmarkAllEngines ./tests/benchmark/...
```

```bash
timeout 15s bash -c 'go clean -testcache && go test -v -race -run=TestClientsLocally ./tests/integration/...'
```

```bash
timeout 15s bash -c 'go clean -testcache && go test -v -race -run=^$ -bench=BenchmarkAllEngines ./tests/benchmark/...'
```

```bash
mkdir -p "./docs/outputs/$(date +'%y-%m-%d')" && \
timeout 5s bash -c 'go clean -testcache && go test -v -race -run=TestClients ./tests/integration/...' \
> "./docs/outputs/$(date +'%y-%m-%d/%H:%M:%S').txt" && \
timeout 5s bash -c 'go clean -testcache && go test -v -race -run=TestClientsWithinDocker ./tests/benchmark/...' \
> "./docs/outputs/$(date +'%y-%m-%d/%H:%M:%S').txt"
```

```bash
go clean -testcache && go test -v -race -run=^$ -bench=BenchmarkAllEngines ./tests/benchmark/...
```

```bash
go clean -testcache && go test -v -race -run=^$ -bench=BenchmarkAllEngines ./tests/benchmark/...
```

```bash
go clean -testcache && go test -v -race -run=TestClientsLocally ./tests/integration/...
```

```bash
go clean -testcache && go test -v -race -run=TestClientsWithinDocker ./tests/benchmark/...
```

```bash
#
```
