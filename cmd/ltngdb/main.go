package main

import (
	"context"
	"log"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/env"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	badgerdbenginev4 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/badgerdb/v4"
	ltngdbenginev2 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/ltngdb/v2"
	ltngnodeconfig "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	commonmodel "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
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

	cfg := &ltngnodeconfig.Config{}
	err = env.Load(cfg)
	if err != nil {
		logger.Error(ctx, "failed to load ltng-node configs", "error", err)

		return
	}

	switch commonmodel.ToEngineVersionType(cfg.Node.Engine.Engine) {
	case commonmodel.BadgerDBV4EngineVersionType:
		badgerdbenginev4.StartV4(ctx, cancelFn, cfg, logger, s, binder, os.Exit)
	case commonmodel.LightningEngineV2EngineVersionType:
		fallthrough
	case commonmodel.DefaultEngineVersionType:
		fallthrough
	default:
		ltngdbenginev2.StartV2(ctx, cancelFn, cfg, logger, s, binder, os.Exit)
	}
}
