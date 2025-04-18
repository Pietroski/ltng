package ltngqueue_mappers

import (
	"time"

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
			Name:                  event.Queue.Name,
			Path:                  event.Queue.Path,
			QueueDistributionType: grpc_ltngqueue.QueueDistributionType(event.Queue.QueueDistributionType),
			CreatedAt:             timestamppb.New(time.Unix(event.Queue.CreatedAt, 0)),
			LastStartedAt:         timestamppb.New(time.Unix(event.Queue.LastStartedAt, 0)),
			Group:                 nil,
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
			SentAt:     timestamppb.New(time.Unix(event.Metadata.SentAt, 0)),
			ReceivedAt: timestamppb.New(time.Unix(event.Metadata.ReceivedAt, 0)),
		}

		if len(event.Metadata.ReceivedAtList) > 0 {
			eventMessage.Event.Metadata.ReceivedAtList =
				make([]*timestamppb.Timestamp, len(event.Metadata.ReceivedAtList))
			for idx, timestamp := range event.Metadata.ReceivedAtList {
				eventMessage.Event.Metadata.ReceivedAtList[idx] = timestamppb.New(time.Unix(timestamp, 0))
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
		Name:                  q.GetName(),
		Path:                  q.GetPath(),
		QueueDistributionType: int32(q.GetQueueDistributionType()), // queuemodels.QueueDistributionType(q.GetQueueDistributionType()),
		CreatedAt:             q.GetCreatedAt().AsTime().Unix(),
		LastStartedAt:         q.GetLastStartedAt().AsTime().Unix(),
	}

	if q.Group != nil {
		queue.Group = &queuemodels.Group{
			Name: q.GetGroup().GetName(),
		}
	}

	return queue
}

func MapToEvent(q *grpc_ltngqueue.Event) *queuemodels.Event {
	if q == nil {
		return nil
	}

	event := &queuemodels.Event{
		EventID: q.GetEventId(),
		Data:    q.GetData(),
	}

	if q.Queue != nil {
		event.Queue = MapToQueue(q.GetQueue())
	}

	if q.Metadata != nil {
		event.Metadata = MapToMetadata(q.GetMetadata())
	}

	return event
}

func MapToMetadata(m *grpc_ltngqueue.EventMetadata) *queuemodels.EventMetadata {
	if m == nil {
		return nil
	}

	eventMetadata := &queuemodels.EventMetadata{
		Metadata:       m.GetMetadata(),
		RetryCount:     m.GetRetryCount(),
		SentAt:         m.GetSentAt().AsTime().Unix(),
		ReceivedAt:     m.GetReceivedAt().AsTime().Unix(),
		ReceivedAtList: nil,
	}

	if len(m.ReceivedAtList) > 0 {
		eventMetadata.ReceivedAtList = make([]int64, len(m.ReceivedAtList))
		for idx, timestamp := range m.ReceivedAtList {
			eventMetadata.ReceivedAtList[idx] = timestamp.AsTime().Unix()
		}
	}

	return eventMetadata
}

func MapFromEvent(e *queuemodels.Event) *grpc_ltngqueue.Event {
	if e == nil {
		return nil
	}

	event := &grpc_ltngqueue.Event{
		EventId:  e.EventID,
		Queue:    MapFromQueue(e.Queue),
		Data:     e.Data,
		Metadata: MapFromMetadata(e.Metadata),
	}

	return event
}

func MapFromQueue(q *queuemodels.Queue) *grpc_ltngqueue.Queue {
	if q == nil {
		return nil
	}

	queue := &grpc_ltngqueue.Queue{
		Name:                  q.Name,
		Path:                  q.Path,
		QueueDistributionType: grpc_ltngqueue.QueueDistributionType(q.QueueDistributionType),
		CreatedAt:             timestamppb.New(time.Unix(q.CreatedAt, 0)),
		LastStartedAt:         timestamppb.New(time.Unix(q.LastStartedAt, 0)),
		Group:                 nil,
	}

	if q.Group != nil {
		queue.Group = &grpc_ltngqueue.Group{
			Name: q.Group.Name,
		}
	}

	return queue
}

func MapFromMetadata(m *queuemodels.EventMetadata) *grpc_ltngqueue.EventMetadata {
	if m == nil {
		return nil
	}

	eventMetadata := &grpc_ltngqueue.EventMetadata{
		Metadata:       m.Metadata,
		RetryCount:     m.RetryCount,
		SentAt:         timestamppb.New(time.Unix(m.SentAt, 0)),
		ReceivedAt:     timestamppb.New(time.Unix(m.ReceivedAt, 0)),
		ReceivedAtList: nil,
	}

	if len(m.ReceivedAtList) > 0 {
		eventMetadata.ReceivedAtList = make([]*timestamppb.Timestamp, len(m.ReceivedAtList))
		for idx, timestamp := range m.ReceivedAtList {
			eventMetadata.ReceivedAtList[idx] = timestamppb.New(time.Unix(timestamp, 0))
		}
	}

	return eventMetadata
}
