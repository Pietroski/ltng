package ltngqueue_engine

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

type Queue struct {
	ctx context.Context

	opMtx *lock.EngineLock
	mtx   *sync.RWMutex

	serializer     serializer_models.Serializer
	queueInfoStore *ltngenginemodels.StoreInfo
	fileManager    *rw.FileManager
	ltngdbengine   *ltng_engine_v2.LTNGEngine
	fqMapping      *safe.GenericMap[*filequeuev1.FileQueue]

	senderGroupMapping *safe.GenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]]
}

func New(ctx context.Context, opts ...options.Option) (*Queue, error) {
	ltngdbengine, err := ltng_engine_v2.New(ctx)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		ctx:   ctx,
		opMtx: lock.NewEngineLock(),
		mtx:   &sync.RWMutex{},

		serializer:   serializer.NewRawBinarySerializer(),
		fileManager:  rw.NewFileManager(ctx),
		ltngdbengine: ltngdbengine,
		fqMapping:    safe.NewGenericMap[*filequeuev1.FileQueue](),

		senderGroupMapping: safe.NewGenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]](),
	}

	queueStoreInfo, err := q.runQueueMigration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run migration: %w", err)
	}
	q.queueInfoStore = queueStoreInfo

	options.ApplyOptions(q, opts...)

	return q, nil
}

func (q *Queue) CreateQueue(ctx context.Context, queue *queuemodels.Queue) (*filequeuev1.FileQueue, error) {
	if err := queue.Validate(); err != nil {
		return nil, fmt.Errorf("error validating queue: %w", err)
	}

	fq, err := filequeuev1.New(ctx, queue.Path, queue.Name)
	if err != nil {
		return nil, fmt.Errorf("error creating queue: %w", err)
	}
	q.fqMapping.Set(queue.GetLockKey(), fq)

	_, err = q.saveQueueReferenceOnDB(ctx, queue)
	if err != nil {
		return nil, fmt.Errorf("error persisting queue: %w", err)
	}

	if _, err = q.CreateQueueGroup(ctx, queue); err != nil {
		rmErr := q.RemoveQueue(ctx, queue)
		if rmErr != nil {
			slog.Error("error removing queue: %w", err)
		}
		return nil, fmt.Errorf("error creating queue group: %w", err)
	}

	return fq, nil
}

func (q *Queue) CreateQueueGroup(ctx context.Context, queue *queuemodels.Queue) (*filequeuev1.FileQueue, error) {
	if queue.Group == nil {
		return nil, nil
	}

	fq, err := filequeuev1.New(ctx, queue.Path, queue.GetGroupName())
	if err != nil {
		return nil, fmt.Errorf("error creating queue: %w", err)
	}
	q.fqMapping.Set(queue.GetCompleteLockKey(), fq)

	return fq, nil
}

func (q *Queue) saveQueueReferenceOnDB(
	ctx context.Context, queue *queuemodels.Queue,
) (*ltngenginemodels.Item, error) {
	lockKey := queue.GetLockKey()
	bs, err := q.serializer.Serialize(queue)
	if err != nil {
		log.Printf("error serializing queue: %v", err)
	}

	dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
	item := &ltngenginemodels.Item{
		Key:   []byte(lockKey),
		Value: bs,
	}
	opts := &ltngenginemodels.IndexOpts{
		HasIdx: false,
	}

	if queue.Group != nil {
		completeLockKey := queue.GetCompleteLockKey()
		opts = &ltngenginemodels.IndexOpts{
			HasIdx:       true,
			ParentKey:    []byte(lockKey),
			IndexingKeys: [][]byte{[]byte(lockKey), []byte(completeLockKey)},
		}
	}

	item, err = q.ltngdbengine.CreateItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		log.Printf("error creating queue item: %v", err)
	}

	return item, nil
}

func (q *Queue) RemoveQueue(ctx context.Context, queue *queuemodels.Queue) error {
	// TODO: finish consuming the queue items or offload them into the db to preserve the history?
	// TODO: should the history be kept or deleted? Should the history be deleted into another request?

	lockKey := queue.GetLockKey()
	dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
	item := &ltngenginemodels.Item{
		Key: []byte(lockKey),
	}
	opts := &ltngenginemodels.IndexOpts{
		HasIdx: false,
	}
	_, err := q.ltngdbengine.DeleteItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		log.Printf("error loading queue item: %v", err)
	}
	q.fqMapping.Delete(queue.GetLockKey())

	return nil
}

func (q *Queue) getQueue(ctx context.Context, queue *queuemodels.Queue) (*filequeuev1.FileQueue, error) {
	lockKey := queue.GetCompleteLockKey()
	fq, ok := q.fqMapping.Get(lockKey)
	if !ok {
		dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
		item := &ltngenginemodels.Item{
			Key: []byte(lockKey),
		}
		opts := &ltngenginemodels.IndexOpts{
			HasIdx: false,
		}
		queueItem, err := q.ltngdbengine.LoadItem(ctx, dbMetaInfo, item, opts)
		if err != nil {
			log.Printf("error loading queue item: %v", err)
		}

		var queueInfo *queuemodels.Queue
		if err = q.serializer.Deserialize(queueItem.Value, &queueInfo); err != nil {
			log.Printf("error deserializing queue item: %v", err)
		}

		fq, err = q.CreateQueue(ctx, queueInfo)
		if err != nil {
			log.Printf("error creating queue: %v", err)
		}

		return fq, nil
	}

	return fq, nil
}

func (q *Queue) Publish(ctx context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("error validating event: %w", err)
	}

	newEventUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("error generating uuid: %w", err)
	}
	event.EventID = newEventUUID.String()
	event.Metadata.ReceivedAt = time.Now()

	fw, err := q.getQueue(ctx, event.Queue)
	if err != nil {
		return nil, fmt.Errorf("error getting queue: %w", err)
	}
	if err = fw.WriteOnCursor(ctx, event); err != nil {
		return nil, fmt.Errorf("error writing event: %w", err)
	}

	return event, nil
}

func (q *Queue) SubscribeToQueue(
	_ context.Context,
	queue *queuemodels.Queue,
	publisher *queuemodels.Publisher,
) {
	lockKey := queue.GetLockKey()
	q.opMtx.Lock(lockKey, struct{}{})
	defer q.opMtx.Unlock(lockKey)
	completeLockKey := queue.GetCompleteLockKey()

	subscriptionQueue, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		subscriptionQueue = safe.NewGenericMap[*queuemodels.QueueOrchestrator]()
		q.senderGroupMapping.Set(lockKey, subscriptionQueue)
	}

	subscriptionQueueGroup, isOk := subscriptionQueue.Get(completeLockKey)
	if !isOk {
		subscriptionQueueGroup = &queuemodels.QueueOrchestrator{
			Queue: queue,
			PublishList: safe.NewTicketStorageLoop[*queuemodels.Publisher](
				safe.WithTicketStorageSize[*queuemodels.Publisher](0),
			),
		}

		subscriptionQueueGroup.PublishList.Append(publisher)
		subscriptionQueue.Set(completeLockKey, subscriptionQueueGroup)
		return
	}

	subscriptionQueueGroup.PublishList.Append(publisher)
}

func (q *Queue) UnsubscribeToQueue(
	_ context.Context,
	queue *queuemodels.Queue,
	publisher *queuemodels.Publisher,
) error {
	lockKey := queue.GetLockKey()
	q.opMtx.Lock(lockKey, struct{}{})
	defer q.opMtx.Unlock(lockKey)
	completeLockKey := queue.GetCompleteLockKey()

	subscriptionQueue, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		return fmt.Errorf("queue not found for %s", lockKey)
	}
	subscriptionGroupQueue, isOk := subscriptionQueue.Get(completeLockKey)
	if !isOk {
		return fmt.Errorf("group queue not found for %s", lockKey)
	}

	subscriptionGroupQueue.PublishList.FindAndDelete(
		func(item *queuemodels.Publisher) bool {
			if item.NodeID == publisher.NodeID {
				return true
			}

			return false
		})

	return nil
}

func (q *Queue) publishToSubscribers(_ context.Context, event *queuemodels.Event) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("error validating event: %w", err)
	}

	lockKey := event.Queue.GetLockKey()
	groupMapping, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		log.Printf("no available queue subscriber for: %s", lockKey)
		return nil
	}

	if event.Queue != nil {
		completeLockKey := event.Queue.GetCompleteLockKey()
		orchestrator, isOkay := groupMapping.Get(completeLockKey)
		if !isOkay {
			log.Printf("no available group queue subscriber for: %s", completeLockKey)
			return nil
		}

		switch orchestrator.Queue.QueueFanOutType {
		case queuemodels.QueueFanOutTypeQueueFanOUtTypeGroupPropagate:
			for _, publisher := range orchestrator.PublishList.Get() {
				publisher.Sender <- event
			}
		case queuemodels.QueueFanOutTypeQueueFanOUtTypeGroupRoundRobin:
			fallthrough
		default:
			orchestrator.PublishList.Next().Sender <- event
		}

		return nil
	}

	groupMapping.Range(func(key string, orchestrator *queuemodels.QueueOrchestrator) bool {
		switch orchestrator.Queue.QueueFanOutType {
		case queuemodels.QueueFanOutTypeQueueFanOutTypePropagate:
			for _, publisher := range orchestrator.PublishList.Get() {
				publisher.Sender <- event
			}
		case queuemodels.QueueFanOutTypeQueueFanOutTypeRoundRobin:
			fallthrough
		default:
			orchestrator.PublishList.Next().Sender <- event
		}

		return true
	})

	return nil
}

func (q *Queue) Consume(ctx context.Context, queue *queuemodels.Queue) (*queuemodels.Event, error) {
	event, err := q.getEventFromQueue(ctx, queue)
	if err != nil {
		return nil, fmt.Errorf("error getting event from queue: %w", err)
	}
	_ = event

	// TODO: distribute

	return nil, nil
}

func (q *Queue) consumerThread(
	ctx context.Context,
	queue *queuemodels.Queue,
	rcv chan struct{},
) {
	for {
		select {
		case rcv <- struct{}{}:
			event, err := q.getEventFromQueue(ctx, queue)
			if err != nil {
				log.Printf("error getting event from queue: %v", err)
				continue
			}

			if err = q.publishToSubscribers(ctx, event); err != nil {
				log.Printf("error publishing event: %v", err)
			}
		case <-ctx.Done():
			log.Printf("context cancellation: %v", ctx.Err())
			return
		}
	}
}

// Consume consumes the next event message
// TODO: pass a callback function or return the event?
func (q *Queue) getEventFromQueue(ctx context.Context, queue *queuemodels.Queue) (*queuemodels.Event, error) {
	fq, err := q.getQueue(ctx, queue)
	if err != nil {
		return nil, fmt.Errorf("error getting queue: %w", err)
	}

	bs, err := fq.ReadFromCursor(ctx)
	if err != nil {
		return nil, fmt.Errorf("error reading from queue: %w", err)
	}

	// TODO: store into to ack items
	// TODO: store into the DB by timed-base-store

	var event queuemodels.Event
	if err = q.serializer.Deserialize(bs, &event); err != nil {
		return nil, fmt.Errorf("error deserializing event: %w", err)
	}

	return &event, nil
}

func (q *Queue) Ack(ctx context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	//eventID := event.EventID
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message

	return nil, nil
}

func (q *Queue) Nack(ctx context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	//eventID := event.EventID

	// TODO: republish
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message

	return nil, nil
}
