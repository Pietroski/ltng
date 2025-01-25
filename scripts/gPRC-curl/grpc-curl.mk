# gRPC-curl makefile

# store tests
########################################################################################################################

test-create-store:
	./scripts/gPRC-curl/operations/create-store.sh

test-get-store:
	./scripts/gPRC-curl/operations/get-store.sh

test-list-stores:
	./scripts/gPRC-curl/operations/list-stores.sh

# item tests
########################################################################################################################

test-create-item:
	./scripts/gPRC-curl/operations/create-item.sh

test-load-item:
	./scripts/gPRC-curl/operations/load-item.sh

########################################################################################################################
