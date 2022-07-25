package ltng_node_config

type (
	Config struct {
		LTNGNode *LTNGNode `validation:"required"`
	}

	LTNGNode struct {
		LTNGManager  *LTNGManager  `validation:"required"`
		LTNGOperator *LTNGOperator `validation:"required"`
	}

	LTNGManager struct {
		Network string `env-name:"LTNG_MANAGER_NETWORK" validation:"required"`
		Address string `env-name:"LTNG_MANAGER_ADDRESS" validation:"required"`
	}

	LTNGOperator struct {
		Network string `env-name:"LTNG_OPERATOR_NETWORK" validation:"required"`
		Address string `env-name:"LTNG_OPERATOR_ADDRESS" validation:"required"`
	}
)
