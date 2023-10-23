package ltng_client

import grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"

type (
	Addresses struct {
		Manager  string
		Operator string
	}

	Cert struct { // TODO: add cert params
		UseCert bool
	}
)

var (
	// DefaultRetrialOpts exports a default retrial option configuration
	DefaultRetrialOpts = &grpc_ops.RetrialOpts{
		RetrialOnError: true,
		RetrialCount:   2,
	}
)
