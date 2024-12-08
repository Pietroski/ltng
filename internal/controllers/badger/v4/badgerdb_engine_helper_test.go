package badgerdb_controller_v4

import (
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
)

var (
	config = &ltng_node_config.Config{
		Node: &ltng_node_config.Node{
			Engine: &ltng_node_config.Engine{
				Engine: common_model.BadgerDBV4EngineVersionType.String(),
			},
		},
	}
)
