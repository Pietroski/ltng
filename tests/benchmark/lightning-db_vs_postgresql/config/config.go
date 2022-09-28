package ltng_node_config

type (
	Config struct {
		LTNGNode   *LTNGNode   `validation:"required"`
		PostgreSQL *PostgreSQL `validation:"required"`
	}

	LTNGNode struct {
		LTNGManager  *LTNGManager  `validation:"required"`
		LTNGOperator *LTNGOperator `validation:"required"`
	}

	LTNGManager struct {
		Network string `env-name:"LTNG_MANAGER_NETWORK" validation:"required"`
		Port    string `env-name:"LTNG_MANAGER_PORT" validation:"required"`
	}

	LTNGOperator struct {
		Network string `env-name:"LTNG_OPERATOR_NETWORK" validation:"required"`
		Port    string `env-name:"LTNG_OPERATOR_PORT" validation:"required"`
	}

	PostgreSQL struct {
		DriverName     string `env-name:"POSTGRESQL_DRIVE_NAME" validation:"required"`
		DataSourceName string `env-name:"POSTGRESQL_DATA_SOURCE_NAME" validation:"required"`
	}
)
