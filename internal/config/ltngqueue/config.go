package ltngqueue_config

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
		Engine string `env:"LTNG_QUEUE_ENGINE" validation:"required"`
	}

	Server struct {
		Network string `env:"LTNG_QUEUE_SERVER_NETWORK" validation:"required"`
		Port    string `env:"LTNG_QUEUE_SERVER_PORT" validation:"required"`
	}

	UI struct {
		Port string `env:"LTNG_QUEUE_UI_ADDR" validation:"required"`
	}
)
