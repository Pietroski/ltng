# Server Manager

## Overview

The `servermanager` package is designed to manage multiple servers in a Go application. It provides a flexible way to start, stop, and monitor servers, along with features like error handling and profiling.

## Installation

To install the `servermanager` package, run the following command:

```bash
go get gitlab.com/pietroski-software-company/golang/devex/servermanager
```

## Usage

Here's an example of how to use the `servermanager` package:

```go
package main

import (
	"context"

    mocked_transport_handlers "gitlab.com/pietroski-software-company/golang/devex/servermanager/mocks/handlers"
	"gitlab.com/pietroski-software-company/golang/devex/servermanager"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

    svr1 := mocked_transport_handlers.NewMockedNamedRunningServer("server-1")
	svr2 := mocked_transport_handlers.NewMockedNamedRunningServer("server-2")

	handler := servermanager.New(ctx, cancel,
        servermanager.WithServers(servermanager.ServerMapping{
			"server-1": svr1,
			"server-2": svr2,
		},
    )
	// Configure servers and options
	handler.StartServers()
}
```

### Configuration Options

The `servermanager` package provides several configuration options:

- `WithServers`: Adds servers to the handler.
- `WithCustomProfilingServer`: Adds a custom profiling server.
- `WithLogger`: Configures the logger.

### Diagrams

For a deeper understanding of the `servermanager` package's functionality, refer to the [diagrams](diagrams/servermanager_diagrams.md) section.

### Contributing

Contributions are welcome. Please open an issue or submit a pull request.

### License

Specify the license under which the package is released.
