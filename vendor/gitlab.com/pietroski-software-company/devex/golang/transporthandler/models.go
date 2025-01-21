package transporthandler

import "os"

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o ./fakes/fake_handler.go . Handler
//counterfeiter:generate -o ./fakes/fake_Server.go . Server

type (
	Handler interface {
		StartServers()
		StopServers()
	}

	ServerMapping map[string]Server

	Server interface {
		Start() error
		Stop()
	}
)

var OsExit = os.Exit
