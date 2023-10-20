package ltng_node_config

type (
	Config struct {
		LTNGNode *LTNGNode `validation:"required"`
	}

	LTNGNode struct {
		LTNGEngine   *LTNGEngine   `validation:"required"`
		LTNGManager  *LTNGManager  `validation:"required"`
		LTNGOperator *LTNGOperator `validation:"required"`
	}

	LTNGEngine struct {
		Engine string `env-name:"LTNG_ENGINE" validation:"required"`
	}

	LTNGManager struct {
		Network string `env-name:"LTNG_MANAGER_NETWORK" validation:"required"`
		Port    string `env-name:"LTNG_MANAGER_PORT" validation:"required"`
	}

	LTNGOperator struct {
		Network string `env-name:"LTNG_OPERATOR_NETWORK" validation:"required"`
		Port    string `env-name:"LTNG_OPERATOR_PORT" validation:"required"`
	}
)
