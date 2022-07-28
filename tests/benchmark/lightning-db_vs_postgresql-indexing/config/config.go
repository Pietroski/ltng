package ltng_node_config

type (
	Config struct {
		LTNGNode   *LTNGNode   `validation:"required"`
		PostgreSQL *PostgreSQL `validation:"required"`
	}

	LTNGNode struct {
		LTNGManager         *LTNGManager         `validation:"required"`
		LTNGOperator        *LTNGOperator        `validation:"required"`
		LTNGIndexedManager  *LTNGIndexedManager  `validation:"required"`
		LTNGIndexedOperator *LTNGIndexedOperator `validation:"required"`
	}

	LTNGManager struct {
		Network string `env-name:"LTNG_MANAGER_NETWORK" validation:"required"`
		Address string `env-name:"LTNG_MANAGER_ADDRESS" validation:"required"`
	}

	LTNGOperator struct {
		Network string `env-name:"LTNG_OPERATOR_NETWORK" validation:"required"`
		Address string `env-name:"LTNG_OPERATOR_ADDRESS" validation:"required"`
	}

	LTNGIndexedManager struct {
		Network string `env-name:"LTNG_INDEXED_MANAGER_NETWORK" validation:"required"`
		Address string `env-name:"LTNG_INDEXED_MANAGER_ADDRESS" validation:"required"`
	}

	LTNGIndexedOperator struct {
		Network string `env-name:"LTNG_INDEXED_OPERATOR_NETWORK" validation:"required"`
		Address string `env-name:"LTNG_INDEXED_OPERATOR_ADDRESS" validation:"required"`
	}

	PostgreSQL struct {
		DriverName     string `env-name:"POSTGRESQL_DRIVE_NAME" validation:"required"`
		DataSourceName string `env-name:"POSTGRESQL_DATA_SOURCE_NAME" validation:"required"`
	}
)
