# gRPC-curl makefile

# store tests
########################################################################################################################

test-create-store:
	./scripts/gPRC-curl/management/create-store.sh

test-get-store:
	./scripts/gPRC-curl/management/get-store.sh

# item tests
########################################################################################################################

test-create-item:
	./scripts/gPRC-curl/operations/create-item.sh

test-load-item:
	./scripts/gPRC-curl/operations/load-item.sh

########################################################################################################################
