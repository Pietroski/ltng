package ltngqueue_mappers

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	grpc_ltngqueue "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngqueue"
)

func MapToCreateQueueSubscriptionResponse(
	event *queuemodels.Event,
) *grpc_ltngqueue.CreateQueueSubscriptionResponse {
	if event == nil {
		return nil
	}

	eventMessage := &grpc_ltngqueue.CreateQueueSubscriptionResponse{
		Event: &grpc_ltngqueue.Event{
			EventId:  event.EventID,
			Data:     event.Data,
			Queue:    nil,
			Metadata: nil,
		},
	}

	if event.Queue != nil {
		eventMessage.Event.Queue = &grpc_ltngqueue.Queue{
			Name:            event.Queue.Name,
			Path:            event.Queue.Path,
			QueueFanOutType: grpc_ltngqueue.QueueFanOutType(event.Queue.QueueFanOutType),
			CreatedAt:       timestamppb.New(event.Queue.CreatedAt),
			LastStartedAt:   timestamppb.New(event.Queue.LastStartedAt),
			Group:           nil,
		}

		if event.Queue.Group != nil {
			eventMessage.Event.Queue.Group = &grpc_ltngqueue.Group{
				Name: event.Queue.Group.Name,
			}
		}
	}

	if event.Metadata != nil {
		eventMessage.Event.Metadata = &grpc_ltngqueue.EventMetadata{
			Metadata:   event.Metadata.Metadata,
			RetryCount: event.Metadata.RetryCount,
			SentAt:     timestamppb.New(event.Metadata.SentAt),
			ReceivedAt: timestamppb.New(event.Metadata.ReceivedAt),
		}

		if len(event.Metadata.ReceivedAtList) > 0 {
			eventMessage.Event.Metadata.ReceivedAtList =
				make([]*timestamppb.Timestamp, len(event.Metadata.ReceivedAtList))
			for idx, timestamp := range event.Metadata.ReceivedAtList {
				eventMessage.Event.Metadata.ReceivedAtList[idx] = timestamppb.New(timestamp)
			}
		}
	}

	return eventMessage
}

func MapToQueue(q *grpc_ltngqueue.Queue) *queuemodels.Queue {
	if q == nil {
		return nil
	}

	queue := &queuemodels.Queue{
		Name:            q.GetName(),
		Path:            q.GetPath(),
		QueueFanOutType: queuemodels.QueueFanOutType(q.GetQueueFanOutType()),
		CreatedAt:       q.GetCreatedAt().AsTime(),
		LastStartedAt:   q.GetLastStartedAt().AsTime(),
	}

	if q.Group != nil {
		queue.Group = &queuemodels.Group{
			Name: q.GetGroup().GetName(),
		}
	}

	return queue
}
