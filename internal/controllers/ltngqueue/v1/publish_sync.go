package ltngqueue_controller_v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ltngqueue_mappers "gitlab.com/pietroski-software-company/lightning-db/internal/mappers/ltngqueue"
	grpc_ltngqueue "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngqueue"
)

func (c *Controller) PublishSync(
	ctx context.Context,
	request *grpc_ltngqueue.PublishSyncRequest,
) (*grpc_ltngqueue.PublishSyncResponse, error) {
	if request.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "event required")
	}

	var err error
	event := ltngqueue_mappers.MapToEvent(request.GetEvent())
	event, err = c.queueEngine.Publish(ctx, event)
	if err != nil {
		return nil, err
	}
	mappedEvent := ltngqueue_mappers.MapFromEvent(event)

	return &grpc_ltngqueue.PublishSyncResponse{
		Event: mappedEvent,
	}, nil
}
