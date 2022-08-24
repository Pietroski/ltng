package ltng_client

type (
	Addresses struct {
		Manager  string
		Operator string
	}

	Cert struct { // TODO: add cert params
		UseCert bool
	}
)
