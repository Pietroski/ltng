# gRPC-curl makefile

# store tests
########################################################################################################################

test-http-create-store:
	./scripts/curl/operations/create-store.sh

test-http-get-store:
	./scripts/curl/operations/get-store.sh

test-http-list-stores:
	./scripts/curl/operations/list-stores.sh

# item tests
########################################################################################################################

test-http-create-item:
	./scripts/curl/operations/create-item.sh

test-http-load-item:
	./scripts/curl/operations/load-item.sh

########################################################################################################################
