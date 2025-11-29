package db_test

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/env"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	badgerdb_engine_v4 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/badgerdb/v4"
	cmdltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/ltngdbengine/v3"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
)

func main(
	ctx context.Context,
	cancel context.CancelFunc,
	opts ...options.Option,
) {
	var err error
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	s := serializer.NewJsonSerializer()
	cfg := &ltng_node_config.Config{}
	err = env.Load(cfg)
	if err != nil {
		logger.Error(ctx, "failed to load env vars", "error", err)

		return
	}

	switch common_model.ToEngineVersionType(cfg.Node.Engine.Engine) {
	case common_model.BadgerDBV4EngineVersionType:
		badgerdb_engine_v4.StartV4(ctx, cancel, cfg, logger, s, func(code int) {
			logger.Debug(ctx, "os.Exit", "code", code)
		}, opts...)
	case common_model.LightningEngineV3EngineVersionType:
		fallthrough
	case common_model.DefaultEngineVersionType:
		fallthrough
	default:
		cmdltngdbenginev3.StartV3(ctx, cancel, cfg, logger, s, func(code int) {
			logger.Debug(ctx, "os.Exit", "code", code)
		}, opts...)
	}
}
