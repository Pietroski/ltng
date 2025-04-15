package ltngqueue_controller_v1

import (
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"

	ltngqueue_mappers "gitlab.com/pietroski-software-company/lightning-db/internal/mappers/ltngqueue"
	grpc_ltngqueue "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngqueue"
)

func (c *Controller) PublishAsync(
	request grpc.ClientStreamingServer[grpc_ltngqueue.PublishAsyncRequest,
		grpc_ltngqueue.PublishAsyncResponse],
) error {
	for {
		req, err := request.Recv()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("error reading from LTNG queue: %w", err)
			}

			log.Printf("error receiving request: %v", err)
			continue
		}

		event := ltngqueue_mappers.MapToEvent(req.GetEvent())
		_, err = c.queueEngine.Publish(request.Context(), event)
		if err != nil {
			log.Printf("error publishing event: %v", err)
		}
	}
}
