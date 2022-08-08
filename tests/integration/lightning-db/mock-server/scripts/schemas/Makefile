# Schema scripts

## creates a swagger folder if does not exists
make-swagger-dir-for-tests-integration-ltngdb:
	@mkdir -p ./tests/integration/lightning-db/mock-server/schemas/generated/swagger

## install all gRPC go plug-ins
go-plugins-for-tests-integration-ltngdb:
	@./tests/integration/lightning-db/mock-server/scripts/schemas/plugins/go-plugins.sh

## generates protobuf
proto-gen-for-tests-integration-ltngdb: make-swagger-dir
	@./tests/integration/lightning-db/mock-server/scripts/schemas/proto-gen/gen.sh
