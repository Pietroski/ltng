package ltngqueue_engine

import (
	"context"
	"errors"
	"fmt"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/errorsx"
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

const signalTransmitterBufferSize = 2 << 15

var notImplemented = fmt.Errorf("not implemented")

type Queue struct {
	ctx context.Context

	opMtx *lock.EngineLock
	mtx   *sync.RWMutex

	awaitTimeout time.Duration

	serializer  serializer_models.Serializer
	fileManager *rw.FileManager

	// this is the in-memory store information
	queueInfoStore *ltngenginemodels.StoreInfo
	ltngdbengine   *ltng_engine_v2.LTNGEngine

	fqMainMapping       *safe.GenericMap[*queuemodels.QueuePublisher]
	fqDownstreamMapping *safe.GenericMap[*queuemodels.QueueSignaler]

	senderGroupMapping *safe.GenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]]
	eventMapTracker    *safe.GenericMap[*queuemodels.EventTracker]

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

		serializer:  serializer.NewRawBinarySerializer(),
		fileManager: rw.NewFileManager(ctx),

		ltngdbengine: ltngdbengine,

		fqMainMapping:       safe.NewGenericMap[*queuemodels.QueuePublisher](),
		fqDownstreamMapping: safe.NewGenericMap[*queuemodels.QueueSignaler](),

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

// Close awaits to process all items and closes:
// - the main file-queue;
// - the downstream file-queues;
// - the db engine core.
func (q *Queue) Close() error {
	q.fqMainMapping.Range(func(key string, value *queuemodels.QueuePublisher) bool {
		for !value.IsClosed.Load() {
			runtime.Gosched()
		}

		return true
	})

	q.fqDownstreamMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
		for !value.IsClosed.Load() {
			runtime.Gosched()
		}

		return true
	})

	q.ltngdbengine.Close()

	return nil
}

// CreateQueuePublisher creates a queue publisher.
// The queue publisher is the first point of contact with the ltng-queue.
// It is responsible for making sure the message was received and stored successfully
// to be later processed.
// Right first, process later, principle.
func (q *Queue) CreateQueuePublisher(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueuePublisher, error) {
	if err := queue.Validate(); err != nil {
		return nil, fmt.Errorf("error validating queue: %w", err)
	}

	if _, err := q.createQueueStoreOnDB(ctx, queue); err != nil {
		return nil, fmt.Errorf("error creating queue store: %w", err)
	}

	fq, err := filequeuev1.New(ctx, queuemodels.PublishersSep+queue.Path, queue.Name)
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, fmt.Errorf("error creating queue: %w", err)
	}
	qp := &queuemodels.QueuePublisher{
		FileQueue: fq,

		FirstSent: new(atomic.Bool),
		IsClosed:  new(atomic.Bool),
	}
	q.fqMainMapping.Set(queue.GetLockKey(), qp)

	_, err = q.saveQueueReferenceOnDB(ctx, queue)
	if err != nil {
		return nil, fmt.Errorf("error persisting queue: %w", err)
	}

	go q.readerPool(q.ctx, qp)

	// create queue signalers
	if _, err = q.CreateQueueSignaler(ctx, queue); err != nil {
		//rmErr := q.deleteQueueReferenceFromDB(ctx, queue)
		//if rmErr != nil {
		//	log.Printf("error removing queue: %v", err)
		//}

		return nil, fmt.Errorf("error creating queue group: %w", err)
	}

	return qp, nil
}

func (q *Queue) CreateQueueSignaler(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	if err := queue.Validate(); err != nil {
		return nil, fmt.Errorf("error validating queue: %w", err)
	}

	fq, err := filequeuev1.New(ctx, queuemodels.SignalersSep+queue.Path, queue.Name)
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, fmt.Errorf("error creating queue: %w", err)
	}

	qs := &queuemodels.QueueSignaler{
		FileQueue:         fq,
		SignalTransmitter: make(chan struct{}, signalTransmitterBufferSize),
		FirstSent:         new(atomic.Bool),
		IsClosed:          new(atomic.Bool),
	}
	q.fqDownstreamMapping.Set(queue.GetLockKey(), qs)

	// TODO: check its functionality
	go q.consumerThread(q.ctx, qs)

	if _, err = q.CreateQueueSignalerGroup(ctx, queue); err != nil {
		rmErr := q.deleteQueueIndexReferenceFromDB(ctx, queue)
		if rmErr != nil {
			log.Printf("error removing queue: %v", err)
		}

		q.fqDownstreamMapping.Delete(queue.GetCompleteLockKey())

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

	fq, err := filequeuev1.New(ctx, queuemodels.SignalersSep+queue.Path, queue.GetGroupName())
	if err != nil {
		return nil, fmt.Errorf("error creating queue: %w", err)
	}

	qs := &queuemodels.QueueSignaler{
		FileQueue:         fq,
		SignalTransmitter: make(chan struct{}, signalTransmitterBufferSize),
		FirstSent:         new(atomic.Bool),
		IsClosed:          new(atomic.Bool),
	}
	q.fqDownstreamMapping.Set(queue.GetCompleteLockKey(), qs)

	// TODO: check its functionality
	go q.consumerThread(q.ctx, qs)

	return qs, nil
}

// createQueueStoreOnDB is responsible to hold the queue messages for historical reasons.
// with that we can query the messages from the queue store.
func (q *Queue) createQueueStoreOnDB(
	ctx context.Context, queue *queuemodels.Queue,
) (*ltngenginemodels.StoreInfo, error) {
	info := &ltngenginemodels.StoreInfo{
		Name:         queue.Name,
		Path:         queue.Path,
		CreatedAt:    time.Now().UTC().Unix(),
		LastOpenedAt: time.Now().UTC().Unix(),
	}
	if _, err := q.ltngdbengine.CreateStore(ctx, info); err != nil {
		return nil, fmt.Errorf("error creating queue: %w", err)
	}

	return info, nil
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

	item, err = q.ltngdbengine.UpsertItem(ctx, dbMetaInfo, item, opts)
	if err != nil && !strings.Contains(err.Error(), "file already exist") {
		log.Printf("error creating queue item: %v", err)
	}

	return item, nil
}

func (q *Queue) deleteQueueReferenceFromDB(
	ctx context.Context, queue *queuemodels.Queue,
) error {
	lockKey := queue.GetLockKey()
	dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
	item := &ltngenginemodels.Item{
		Key: []byte(lockKey),
	}
	opts := &ltngenginemodels.IndexOpts{
		HasIdx: false,
	}
	//if queue.Group != nil {
	//	completeLockKey := queue.GetCompleteLockKey()
	//	opts = &ltngenginemodels.IndexOpts{
	//		HasIdx:       true,
	//		IndexingKeys: [][]byte{[]byte(lockKey), []byte(completeLockKey)},
	//		IndexProperties: ltngenginemodels.IndexProperties{
	//			IndexDeletionBehaviour: ltngenginemodels.CascadeByIdx,
	//		},
	//	}
	//}
	_, err := q.ltngdbengine.DeleteItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		log.Printf("error loading queue item: %v", err)
	}

	return nil
}

func (q *Queue) deleteQueueIndexReferenceFromDB(
	ctx context.Context, queue *queuemodels.Queue,
) error {
	if queue.Group == nil {
		return nil
	}

	lockKey := queue.GetLockKey()
	dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
	item := &ltngenginemodels.Item{
		Key: []byte(lockKey),
	}
	completeLockKey := queue.GetCompleteLockKey()
	opts := &ltngenginemodels.IndexOpts{
		HasIdx:       true,
		IndexingKeys: [][]byte{[]byte(completeLockKey)},
		IndexProperties: ltngenginemodels.IndexProperties{
			IndexDeletionBehaviour: ltngenginemodels.IndexOnly,
		},
	}
	_, err := q.ltngdbengine.DeleteItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		log.Printf("error loading queue item: %v", err)
	}

	return nil
}

func (q *Queue) deleteQueuesFromInMemoryMaps(
	_ context.Context, queue *queuemodels.Queue,
) {
	q.fqMainMapping.Delete(queue.GetLockKey())
	q.fqDownstreamMapping.Delete(queue.GetLockKey())
	q.fqDownstreamMapping.Delete(queue.GetCompleteLockKey())
}

func (q *Queue) DeleteQueueHistory(
	ctx context.Context, queue *queuemodels.Queue,
) error {
	return notImplemented
}

func (q *Queue) getQueuePublisher(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueuePublisher, error) {
	lockKey := queue.GetLockKey()
	qp, ok := q.fqMainMapping.Get(lockKey)
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

		qp, err = q.CreateQueuePublisher(ctx, queueInfo)
		if err != nil {
			log.Printf("error creating queue: %v", err)
		}

		return qp, nil
	}

	return qp, nil
}

func (q *Queue) getQueueSignaler(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	lockKey := queue.GetCompleteLockKey()
	qs, ok := q.fqDownstreamMapping.Get(lockKey)
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

	qp, err := q.getQueuePublisher(ctx, event.Queue)
	if err != nil {
		return nil, fmt.Errorf("error getting queue: %w", err)
	}

	if err = qp.FileQueue.WriteOnCursor(ctx, event); err != nil {
		return nil, fmt.Errorf("error writing event: %w", err)
	}

	return event, nil
}

// publishDownStream publishes a message to downstream queues.
// It constantly keeps reading the file until there is something to be read.
func (q *Queue) publishDownStream(
	ctx context.Context, event *queuemodels.Event,
) (*queuemodels.Event, error) {
	//if err := event.Validate(); err != nil {
	//	return nil, fmt.Errorf("error validating event: %w", err)
	//}
	//
	//if event.EventID == "" {
	//	newEventUUID, err := uuid.NewRandom()
	//	if err != nil {
	//		return nil, fmt.Errorf("error generating uuid: %w", err)
	//	}
	//
	//	event.EventID = newEventUUID.String()
	//}
	//
	//receivedAtTime := time.Unix(event.Metadata.ReceivedAt, 0)
	//if receivedAtTime.IsZero() {
	//	event.Metadata.ReceivedAt = time.Now().UTC().Unix()
	//}
	//
	//if event.Metadata.ReceivedAtList == nil {
	//	event.Metadata.ReceivedAtList = []int64{event.Metadata.ReceivedAt}
	//} else {
	//	event.Metadata.ReceivedAtList = append(event.Metadata.ReceivedAtList, time.Now().UTC().Unix())
	//}

	// TODO: context handler

	//qs, ok := q.fqMapping.Get(event.Queue.GetLockKey())
	//if !ok {
	//	return nil, fmt.Errorf("error getting file-queue for downstream event: %v", event.Queue.GetLockKey())
	//}
	//go qs.FileQueue.ReaderPooler(ctx, func(ctx context.Context, bs []byte) error {
	//
	//})
	//
	//qs, err := q.getQueueSignaler(ctx, event.Queue)
	//if err != nil {
	//	return nil, fmt.Errorf("error getting queue: %w", err)
	//}
	//
	//if err = qs.FileQueue.WriteOnCursor(ctx, event); err != nil {
	//	return nil, fmt.Errorf("error writing event: %w", err)
	//}

	//q.fqDownstreamMapping.Get()

	return nil, nil
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

		subscriptionQueue.Set(completeLockKey, subscriptionQueueGroup)
	}
	subscriptionQueueGroup.PublishList.Append(publisher)

	//{
	//	qs, ok := q.fqMapping.Get(lockKey)
	//	if !ok {
	//		return fmt.Errorf("error loading queue subscriber for lockKey: no subscriber found")
	//	}
	//
	//	if qs.FirstSent.CompareAndSwap(false, true) {
	//		qs.SignalTransmitter <- struct{}{}
	//	}
	//}
	//
	//{
	//	if lockKey == completeLockKey {
	//		return nil
	//	}
	//
	//	qs, ok := q.fqMapping.Get(completeLockKey)
	//	if !ok {
	//		return fmt.Errorf("error loading queue subscriber for completeLockKey: no subscriber found")
	//	}
	//
	//	if qs.FirstSent.CompareAndSwap(false, true) {
	//		qs.SignalTransmitter <- struct{}{}
	//	}
	//}

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
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT:
			for _, publisher := range orchestrator.PublishList.Get() {
				publisher.Sender <- event
			}
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN:
			fallthrough
		default:
			orchestrator.PublishList.Next().Sender <- event
		}

		return nil
	}

	//TODO: remove comments
	groupMapping.Range(func(key string, orchestrator *queuemodels.QueueOrchestrator) bool {
		//fmt.Printf("publishing to queue: %s - %v - %v - %+v\n", key,
		//	orchestrator.Queue.QueueDistributionType, orchestrator.Queue, orchestrator.PublishList.Get())
		switch orchestrator.Queue.QueueDistributionType {
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT:
			//fmt.Printf("publishing to fan out: %s - %v\n", key, orchestrator.PublishList.Get())
			for _, publisher := range orchestrator.PublishList.Get() {
				publisher.Sender <- event
			}
		case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN:
			fallthrough
		default:
			//fmt.Printf("publishing to round robin: %s\n", key)
			orchestrator.PublishList.Next().Sender <- event
		}

		return true
	})

	return nil
}

func (q *Queue) readerPool(
	ctx context.Context,
	queuePublisher *queuemodels.QueuePublisher,
) {
	err := queuePublisher.FileQueue.ReaderPooler(ctx, func(ctx context.Context, bs []byte) error {
		var event queuemodels.Event
		if err := q.serializer.Deserialize(bs, &event); err != nil {
			return fmt.Errorf(
				"error deserializing event from file queue: %w: %w",
				err, errorsx.ErrUnRetryable)
		}

		fmt.Printf("event: %+v\n", event)

		// TODO: implement the downstream publishing logic

		//

		return nil
	})
	if err != nil {
		log.Printf("error reading/closing from queue: %s", err)
	}

	queuePublisher.IsClosed.Store(true)
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
		EventID:   event.EventID,
		Event:     event,
		Ack:       ack,
		Nack:      nack,
		WasACKed:  new(atomic.Bool),
		WasNACKed: new(atomic.Bool),
	}
	fmt.Printf("event %v\n", event.EventID)
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
			if err := queueSignaler.FileQueue.PopFromIndex(ctx, eventIndex); err != nil {
				log.Printf("error popping queue: %v", err)
			}

			// TODO: debug log
			// log.Printf("event %v successfully ack'ed", event.EventID)

			q.eventMapTracker.Delete(event.EventID)

			return
		case <-nack:
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

			q.eventMapTracker.Delete(event.EventID)

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

			q.eventMapTracker.Delete(event.EventID)

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
	//if bs == nil || len(bs) == 0 {
	//	return nil, io.EOF
	//}

	// TODO: store into to ack items
	// TODO: store into the DB by timed-base-store

	var event queuemodels.Event
	if err = q.serializer.Deserialize(bs, &event); err != nil {
		return nil, fmt.Errorf("error deserializing event: %w", err)
	}

	return &event, nil
}

func (q *Queue) Ack(_ context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	q.opMtx.Lock(event.EventID, struct{}{})
	defer q.opMtx.Unlock(event.EventID)

	//eventID := event.EventID
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message
	et, ok := q.eventMapTracker.Get(event.EventID)
	if !ok {
		//return nil, nil
		return nil, fmt.Errorf(
			"no event mapper found for %s or event message already ack'ed - failed to ack event message",
			event.EventID,
		)
	}
	if et.WasACKed.Load() {
		return et.Event, nil
	}

	et.Ack <- struct{}{}
	et.WasACKed.Store(true)

	return et.Event, nil
}

func (q *Queue) Nack(_ context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	q.opMtx.Lock(event.EventID, struct{}{})
	defer q.opMtx.Unlock(event.EventID)

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

// create the topic and subtopic - does nothing if it already exists
// create the subscriber - it can be for a group or the whole queue
// a file queue is consumed, and it posts to the other queues

// Secret Service - Your Service for Services's Secrets
