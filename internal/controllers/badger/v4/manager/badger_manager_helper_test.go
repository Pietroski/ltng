package badgerdb_manager_controller_v4

import (
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	common_model "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/common"
)

var (
	config = &ltng_node_config.Config{
		LTNGNode: &ltng_node_config.LTNGNode{
			LTNGEngine: &ltng_node_config.LTNGEngine{
				Engine: common_model.BadgerDBV4EngineVersionType.String(),
			},
		},
	}
)
