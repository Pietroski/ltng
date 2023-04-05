package handlers_model

type (
	Server interface {
		Start() error
		Stop()
	}
)
