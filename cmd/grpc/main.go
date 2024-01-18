package main

import (
	"context"
	common_model "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/common"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	badgerdb_engine_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/cmd/grpc/badgerdb/v3"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())

	var err error
	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: false,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts).FromCtx(ctx)

	serializer := go_serializer.NewJsonSerializer()
	validator := go_validator.NewStructValidator()
	binder := go_binder.NewStructBinder(serializer, validator)
	chainedOperator := chainded_operator.NewChainOperator()

	cfg := &ltng_node_config.Config{}
	err = go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		logger.Errorf(
			"failed to load ltng node configs",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	switch common_model.ToEngineVersionType(cfg.LTNGNode.LTNGEngine.Engine) {
	case common_model.DefaultEngineVersionType,
		common_model.BadgerDBV3EngineVersionType:
		//params := &badgerdb_engine_v3.V3Params{
		//	Ctx:             ctx,
		//	CancelFn:        cancelFn,
		//	Cfg:             cfg,
		//	Logger:          logger,
		//	Serializer:      serializer,
		//	Binder:          binder,
		//	ChainedOperator: chainedOperator,
		//}

		badgerdb_engine_v3.StartV3(ctx, cancelFn, cfg, logger, serializer, binder, chainedOperator)
	default: // TODO return an error instead?
		badgerdb_engine_v3.StartV3(ctx, cancelFn, cfg, logger, serializer, binder, chainedOperator)
	}
}
