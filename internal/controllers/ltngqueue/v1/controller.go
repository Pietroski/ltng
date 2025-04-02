package ltngqueue_controller_v1

import (
	"context"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltngqueue_engine "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/queue/v1"
	ltngqueue_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngqueue"
	grpc_ltngqueue "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngqueue"
)

type Controller struct {
	grpc_ltngqueue.UnimplementedLightningQueueServer
	ctx          context.Context
	cfg          *ltngqueue_config.Config
	consumerList *ltng_engine_v2.LTNGEngine
	queueEngine  *ltngqueue_engine.Queue
}

func New(ctx context.Context, opts ...options.Option) *Controller {
	ctrl := Controller{}
	options.ApplyOptions(ctrl, opts...)

	return &ctrl
}
