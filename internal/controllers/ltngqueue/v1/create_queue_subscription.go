package ltngqueue_controller_v1

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ltngqueue_mappers "gitlab.com/pietroski-software-company/lightning-db/internal/mappers/ltngqueue"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	grpc_ltngqueue "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngqueue"
)

func (c *Controller) CreateQueueSubscription(
	ctx context.Context,
	request *grpc_ltngqueue.CreateQueueSubscriptionRequest,
	responseStream grpc.ServerStreamingServer[*grpc_ltngqueue.CreateQueueSubscriptionResponse],
) error {
	if request.Queue == nil {
		return status.Error(codes.InvalidArgument, "queue required")
	}
	queue := ltngqueue_mappers.MapToQueue(request.GetQueue())

	nodeUUID, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

	receiver := make(chan *queuemodels.Event, 1)
	publisher := &queuemodels.Publisher{
		NodeID: nodeID,
		Sender: receiver,
	}
	err = c.queueEngine.SubscribeToQueue(ctx, queue, publisher)
	if err != nil {
		return fmt.Errorf("error subscribing to queue: %w", err)
	}

	defer func() {
		if err := c.queueEngine.UnsubscribeToQueue(ctx, queue, publisher); err != nil {
			log.Printf("error unsubscribing from queue: %v: error: %v", publisher.NodeID, err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-receiver:
			if err := responseStream.SendMsg(event); err != nil {
				log.Printf("error sending event message from queue: %v: error: %v", publisher.NodeID, err)
				return err
			}
		}
	}
}
