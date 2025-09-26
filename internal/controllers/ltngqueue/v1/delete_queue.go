package ltngqueue_controller_v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ltngqueue_mappers "gitlab.com/pietroski-software-company/lightning-db/internal/mappers/ltngqueue"
	grpc_ltngqueue "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngqueue"
)

func (c *Controller) DeleteQueue(
	ctx context.Context,
	request *grpc_ltngqueue.DeleteQueueRequest,
) (*grpc_ltngqueue.DeleteQueueResponse, error) {
	if request.Queue == nil {
		return nil, status.Error(codes.InvalidArgument, "queue required")
	}
	queue := ltngqueue_mappers.MapToQueue(request.GetQueue())
	_ = queue

	//if err := c.queueEngine.DeleteQueue(ctx, queue); err != nil {
	//	return nil, err
	//}

	return &grpc_ltngqueue.DeleteQueueResponse{}, nil
}
