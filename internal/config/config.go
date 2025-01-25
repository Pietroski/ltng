package ltng_node_config

type (
	Config struct {
		Node *Node `validation:"required"`
	}

	Node struct {
		Engine *Engine `validation:"required"`
		Server *Server `validation:"required"`
		UI     *UI     `validation:"required"`
	}

	Engine struct {
		Engine string `env-name:"LTNG_ENGINE" validation:"required"`
	}

	Server struct {
		Network string `env-name:"LTNG_SERVER_NETWORK" validation:"required"`
		Port    string `env-name:"LTNG_SERVER_PORT" validation:"required"`
	}

	UI struct {
		Port string `env-name:"LTNG_UI_ADDR" validation:"required"`
	}
)
