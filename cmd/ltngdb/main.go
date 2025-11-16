package main

import (
	"context"
	"log"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/env"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"

	badgerdbenginev4 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/badgerdb/v4"
	cmdltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/ltngdbengine/v3"
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
	s := serializer.NewRawBinarySerializer()

	cfg := &ltngnodeconfig.Config{}
	err = env.Load(cfg)
	if err != nil {
		logger.Error(ctx, "failed to load ltng-node configs", "error", err)

		return
	}

	switch commonmodel.ToEngineVersionType(cfg.Node.Engine.Engine) {
	case commonmodel.BadgerDBV4EngineVersionType:
		badgerdbenginev4.StartV4(ctx, cancelFn, cfg, logger, s, os.Exit)
	case commonmodel.LightningEngineV3EngineVersionType:
		fallthrough
	case commonmodel.DefaultEngineVersionType:
		fallthrough
	default:
		cmdltngdbenginev3.StartV3(ctx, cancelFn, cfg, logger, s, os.Exit)
	}
}
