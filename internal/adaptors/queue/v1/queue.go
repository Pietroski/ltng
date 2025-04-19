package ltngqueue_engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

var notImplemented = fmt.Errorf("not implemented")

type Queue struct {
	ctx context.Context

	opMtx *lock.EngineLock
	mtx   *sync.RWMutex

	awaitTimeout time.Duration

	serializer     serializer_models.Serializer
	queueInfoStore *ltngenginemodels.StoreInfo
	fileManager    *rw.FileManager
	ltngdbengine   *ltng_engine_v2.LTNGEngine
	fqMapping      *safe.GenericMap[*queuemodels.QueueSignaler]

	senderGroupMapping *safe.GenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]]

	eventMapTracker *safe.GenericMap[*queuemodels.EventTracker]

	retryCountLimit uint64
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

		serializer:         serializer.NewRawBinarySerializer(),
		fileManager:        rw.NewFileManager(ctx),
		ltngdbengine:       ltngdbengine,
		fqMapping:          safe.NewGenericMap[*queuemodels.QueueSignaler](),
		senderGroupMapping: safe.NewGenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]](),
		eventMapTracker:    safe.NewGenericMap[*queuemodels.EventTracker](),

		retryCountLimit: 5,
		awaitTimeout:    time.Second * 5,
	}

	queueStoreInfo, err := q.runQueueMigration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run migration: %w", err)
	}
	q.queueInfoStore = queueStoreInfo

	options.ApplyOptions(q, opts...)

	return q, nil
}

func (q *Queue) Close() error {
	q.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
		for !value.IsClosed.Load() {
			runtime.Gosched()
		}

		return true
	})
	q.ltngdbengine.Close()

	return nil
}

func (q *Queue) CreateQueueSignaler(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	if err := queue.Validate(); err != nil {
		return nil, fmt.Errorf("error validating queue: %w", err)
	}

	fq, err := filequeuev1.New(ctx, queue.Path, queue.Name)
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, fmt.Errorf("error creating queue: %w", err)
	}
	qs := &queuemodels.QueueSignaler{
		FileQueue: fq,
		// TODO: revisit!!
		SignalTransmitter: make(chan struct{}),
		FirstSent:         new(atomic.Bool),
		IsClosed:          new(atomic.Bool),
	}
	q.fqMapping.Set(queue.GetLockKey(), qs)

	_, err = q.saveQueueReferenceOnDB(ctx, queue)
	if err != nil {
		return nil, fmt.Errorf("error persisting queue: %w", err)
	}

	go q.consumerThread(q.ctx, qs)

	if _, err = q.CreateQueueSignalerGroup(ctx, queue); err != nil {
		rmErr := q.DeleteQueue(ctx, queue)
		if rmErr != nil {
			log.Printf("error removing queue: %v", err)
		}
		return nil, fmt.Errorf("error creating queue group: %w", err)
	}

	return qs, nil
}

func (q *Queue) CreateQueueSignalerGroup(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	if queue.Group == nil {
		return nil, nil
	}

	fq, err := filequeuev1.New(ctx, queue.Path, queue.GetGroupName())
	if err != nil {
		return nil, fmt.Errorf("error creating queue: %w", err)
	}
	qs := &queuemodels.QueueSignaler{
		FileQueue: fq,
		// TODO: revisit!!
		SignalTransmitter: make(chan struct{}),
		FirstSent:         new(atomic.Bool),
		IsClosed:          new(atomic.Bool),
	}
	q.fqMapping.Set(queue.GetCompleteLockKey(), qs)
	go q.consumerThread(q.ctx, qs)

	return qs, nil
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
	if err != nil && !strings.Contains(err.Error(), "file already exist") {
		log.Printf("error creating queue item: %v", err)
	}

	return item, nil
}

func (q *Queue) DeleteQueue(ctx context.Context, queue *queuemodels.Queue) error {
	// TODO: finish consuming the queue items or offload them into the db to preserve the history? -
	// TODO: should the history be kept or deleted? Should the history be deleted into another request? - it should be kept

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
	q.fqMapping.Delete(lockKey)
	q.fqMapping.Delete(queue.GetCompleteLockKey())

	return nil
}

func (q *Queue) DeleteQueueHistory(
	ctx context.Context, queue *queuemodels.Queue,
) error {
	return notImplemented
}

func (q *Queue) getQueueSignaler(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	lockKey := queue.GetCompleteLockKey()
	qs, ok := q.fqMapping.Get(lockKey)
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

		qs, err = q.CreateQueueSignaler(ctx, queueInfo)
		if err != nil {
			log.Printf("error creating queue: %v", err)
		}

		return qs, nil
	}

	return qs, nil
}

func (q *Queue) Publish(ctx context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("error validating event: %w", err)
	}

	if event.EventID == "" {
		newEventUUID, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("error generating uuid: %w", err)
		}
		event.EventID = newEventUUID.String()
	}

	receivedAtTime := time.Unix(event.Metadata.ReceivedAt, 0)
	if receivedAtTime.IsZero() {
		event.Metadata.ReceivedAt = time.Now().UTC().Unix()
	}
	if event.Metadata.ReceivedAtList == nil {
		event.Metadata.ReceivedAtList = []int64{event.Metadata.ReceivedAt}
	} else {
		event.Metadata.ReceivedAtList = append(event.Metadata.ReceivedAtList, time.Now().UTC().Unix())
	}

	qs, err := q.getQueueSignaler(ctx, event.Queue)
	if err != nil {
		return nil, fmt.Errorf("error getting queue: %w", err)
	}
	if err = qs.FileQueue.WriteOnCursor(ctx, event); err != nil {
		return nil, fmt.Errorf("error writing event: %w", err)
	}

	return event, nil
}

func (q *Queue) SubscribeToQueue(
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
	} else {
		subscriptionQueueGroup.PublishList.Append(publisher)
	}

	qs, ok := q.fqMapping.Get(completeLockKey)
	if !ok {
		return fmt.Errorf("error loading queue subscriber: no subscriber found")
	}

	if qs.FirstSent.CompareAndSwap(false, true) {
		qs.SignalTransmitter <- struct{}{}
	}

	return nil
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
				// TODO: close sender channel
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

	if event.Queue.Group != nil {
		completeLockKey := event.Queue.GetCompleteLockKey()
		orchestrator, isOkay := groupMapping.Get(completeLockKey)
		if !isOkay {
			log.Printf("no available group queue subscriber for: %s", completeLockKey)
			return nil
		}

		switch orchestrator.Queue.QueueDistributionType {
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_FAN_OUT:
			for _, publisher := range orchestrator.PublishList.Get() {
				publisher.Sender <- event
			}
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN:
			fallthrough
		default:
			orchestrator.PublishList.Next().Sender <- event
		}

		return nil
	}

	groupMapping.Range(func(key string, orchestrator *queuemodels.QueueOrchestrator) bool {
		switch orchestrator.Queue.QueueDistributionType {
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT:
			for _, publisher := range orchestrator.PublishList.Get() {
				publisher.Sender <- event
			}
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN:
			fallthrough
		default:
			orchestrator.PublishList.Next().Sender <- event
		}

		return true
	})

	return nil
}

func (q *Queue) consumerThread(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
) {
	for {
		select {
		case <-queueSignaler.SignalTransmitter:
			event, err := q.getQueueNextEvent(ctx, queueSignaler)
			if err != nil {
				if errors.Is(err, io.EOF) {
					continue
				}

				log.Printf("error getting event from queue: %v", err)
				continue
			}
			// TODO: debug log
			//log.Printf("next event %v", event.EventID)

			go q.handleEventWaiting(ctx, queueSignaler, event)
		case <-ctx.Done():
			log.Printf("server context cancellation: %v", ctx.Err())
			if err := queueSignaler.FileQueue.Reset(); err != nil {
				log.Printf("error resetting file queue: %v", err)
			}

			queueSignaler.IsClosed.Store(true)
			return
		}
	}
}

func (q *Queue) handleEventWaiting(
	ctx context.Context, queueSignaler *queuemodels.QueueSignaler, event *queuemodels.Event,
) {
	ack := make(chan struct{})
	nack := make(chan struct{})
	et := &queuemodels.EventTracker{
		EventID: event.EventID,
		Event:   event,
		Ack:     ack,
		Nack:    nack,
	}
	q.eventMapTracker.Set(event.EventID, et)
	if err := q.publishToSubscribers(ctx, event); err != nil {
		log.Printf("error publishing event: %v", err)
	}
	eventIndex := []byte(et.EventID)

	ctxtimeout, cancel := context.WithTimeout(ctx, q.awaitTimeout)
	defer func() {
		cancel()
		queueSignaler.SignalTransmitter <- struct{}{}
	}()

	for {
		select {
		case <-ack:
			cancel()
			if err := queueSignaler.FileQueue.PopFromIndex(ctx, eventIndex); err != nil {
				log.Printf("error popping queue: %v", err)
			}

			// TODO: debug log
			// log.Printf("event %v successfully ack'ed", event.EventID)
			return
		case <-nack:
			cancel()
			if err := queueSignaler.FileQueue.PopFromIndex(ctx, eventIndex); err != nil {
				log.Printf("error popping queue: %v", err)
				return
			}

			// TODO: add retry count limit
			if event.Metadata.RetryCount > q.retryCountLimit {
				// TODO: log and send it to the DLQ if existent?
				return
			}
			event.Metadata.RetryCount++

			if _, err := q.Publish(ctx, event); err != nil {
				log.Printf("error re-publishing nacked event: %v", err)
			}

			// TODO: debug log
			// log.Printf("event %v successfully nack'ed", event.EventID)

			return
		case <-ctxtimeout.Done():
			err := ctxtimeout.Err()
			if !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("event %v successfully ack'ed", event.EventID)
				return
			}

			// TODO: debug log
			log.Printf("event context cancellation: %v",
				fmt.Errorf("event %v being cancelled: %v", event.EventID, err))

			if err := queueSignaler.FileQueue.RepublishIndex(ctx, eventIndex); err != nil {
				log.Printf("error republishing to queue: %v", err)
			}

			return
		}
	}
}

func (q *Queue) getQueueNextEvent(
	ctx context.Context, queueSignaler *queuemodels.QueueSignaler,
) (*queuemodels.Event, error) {
	bs, err := queueSignaler.FileQueue.ReadFromCursorWithoutTruncation(ctx)
	if err != nil {
		return nil, fmt.Errorf("error reading from queue: %w", err)
	}
	if bs == nil || len(bs) == 0 {
		return nil, io.EOF
	}

	// TODO: store into to ack items
	// TODO: store into the DB by timed-base-store

	var event queuemodels.Event
	if err = q.serializer.Deserialize(bs, &event); err != nil {
		return nil, fmt.Errorf("error deserializing event: %w", err)
	}

	return &event, nil
}

func (q *Queue) Ack(_ context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	//eventID := event.EventID
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message
	et, ok := q.eventMapTracker.Get(event.EventID)
	if !ok {
		return nil, fmt.Errorf("no event mapper found for %s - failed to ack event message", event.EventID)
	}
	et.Ack <- struct{}{}

	return et.Event, nil
}

func (q *Queue) Nack(_ context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	//eventID := event.EventID
	// TODO: republish
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message

	et, ok := q.eventMapTracker.Get(event.EventID)
	if !ok {
		return nil, fmt.Errorf("no event mapper found for %s - failed to ack event message", event.EventID)
	}
	et.Nack <- struct{}{}

	return nil, nil
}
