package operation_models

type (
	OpItem struct {
		Key   []byte
		Value []byte
	}

	OpList []*OpItem
)
