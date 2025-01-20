package main

import (
	"context"
	"os"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	badgerdb_engine_v4 "gitlab.com/pietroski-software-company/lightning-db/cmd/grpc/badgerdb/v4"
	ltngdb_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/cmd/grpc/ltngdb/v1"
	ltngdb_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/cmd/grpc/ltngdb/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
)

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	var err error
	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: false,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts).FromCtx(ctx)

	s := serializer.NewJsonSerializer()
	validator := go_validator.NewStructValidator()
	binder := go_binder.NewStructBinder(s, validator)

	cfg := &ltng_node_config.Config{}
	err = go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		logger.Errorf(
			"failed to load ltng's node configs",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	switch common_model.ToEngineVersionType(cfg.Node.Engine.Engine) {
	case common_model.BadgerDBV4EngineVersionType:
		badgerdb_engine_v4.StartV4(ctx, cancelFn, cfg, logger, s, binder, os.Exit)
	case common_model.LightningEngineV1EngineVersionType:
		ltngdb_engine_v1.StartV1(ctx, cancelFn, cfg, logger, s, binder)
	case common_model.LightningEngineV2EngineVersionType:
		fallthrough
	case common_model.DefaultEngineVersionType:
		fallthrough
	default:
		ltngdb_engine_v2.StartV2(ctx, cancelFn, cfg, logger, s, binder, os.Exit)
	}
}
