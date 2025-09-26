package servermanager

import "os"

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

	Exiter interface {
		Exit(code int)
	}
)

var OsExit = os.Exit

func (sm ServerMapping) NameList() []string {
	names := make([]string, 0, len(sm))
	for name := range sm {
		names = append(names, name)
	}

	return names
}
