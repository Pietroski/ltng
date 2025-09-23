package main

import (
	"context"
	"log"
	"os"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	ltngdb_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/ltngdb/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
)

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	ctx, err := tracer.New().Trace(ctx)
	if err != nil {
		log.Fatal(err)
	}

	logger := slogx.New() // slogx.WithLogLevel(slog.LevelDebug)

	s := serializer.NewJsonSerializer()
	validator := go_validator.NewStructValidator()
	binder := go_binder.NewStructBinder(s, validator)

	cfg := &ltng_node_config.Config{}
	err = go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		logger.Error(ctx, "failed to load ltng-node configs", "error", err)

		return
	}

	switch common_model.ToEngineVersionType(cfg.Node.Engine.Engine) {
	case common_model.BadgerDBV4EngineVersionType:
		//badgerdb_engine_v4.StartV4(ctx, cancelFn, cfg, logger, s, binder, os.Exit)
	case common_model.LightningEngineV2EngineVersionType:
		fallthrough
	case common_model.DefaultEngineVersionType:
		fallthrough
	default:
		ltngdb_engine_v2.StartV2(ctx, cancelFn, cfg, logger, s, binder, os.Exit)
	}
}
