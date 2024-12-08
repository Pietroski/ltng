package ltng_client

import grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"

type (
	Cert struct { // TODO: add cert params
		UseCert bool
	}
)

var (
	// DefaultRetrialOpts exports a default retrial option configuration
	DefaultRetrialOpts = &grpc_ltngdb.RetrialOpts{
		RetrialOnError: true,
		RetrialCount:   2,
	}
)
