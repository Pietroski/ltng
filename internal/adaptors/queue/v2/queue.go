package ltngqueue_engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/ctx/ctxhandler"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/errorsx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

const signalTransmitterBufferSize = 1 << 4

var (
	notImplemented         = fmt.Errorf("not implemented")
	noAvailableSubscribers = fmt.Errorf("no available subscribers")
)

type Queue struct {
	ctx context.Context

	opMtx *lock.EngineLock
	mtx   *sync.RWMutex
	//op    *concurrent.OffThread

	awaitTimeout time.Duration

	serializer  serializer_models.Serializer
	fileManager *rw.FileManager

	// this is the in-memory store information
	queueInfoStore *ltngenginemodels.StoreInfo
	ltngdbengine   *ltng_engine_v2.LTNGEngine

	fqMainMapping       *safe.GenericMap[*queuemodels.QueuePublisher]
	fqDownstreamMapping *safe.GenericMap[*safe.GenericMap[*queuemodels.QueueSignaler]]

	senderGroupMapping *safe.GenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]]
	eventMapTracker    *safe.GenericMap[*queuemodels.EventTracker]

	retryCountLimit uint64

	timer func() time.Time
}

func New(ctx context.Context, opts ...options.Option) (*Queue, error) {
	ltngdbengine, err := ltng_engine_v2.New(ctx)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		ctx: ctx,

		opMtx: lock.NewEngineLock(),
		mtx:   &sync.RWMutex{},
		//op:    concurrent.New("ltng-queue"),

		serializer:  serializer.NewRawBinarySerializer(),
		fileManager: rw.NewFileManager(ctx),

		ltngdbengine: ltngdbengine,

		fqMainMapping:       safe.NewGenericMap[*queuemodels.QueuePublisher](),
		fqDownstreamMapping: safe.NewGenericMap[*safe.GenericMap[*queuemodels.QueueSignaler]](),

		senderGroupMapping: safe.NewGenericMap[*safe.GenericMap[*queuemodels.QueueOrchestrator]](),
		eventMapTracker:    safe.NewGenericMap[*queuemodels.EventTracker](),

		retryCountLimit: 5,
		awaitTimeout:    time.Second * 5,

		timer: time.Now,
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
	// give it a delay before shutting down if the service crashes at the beginning
	time.Sleep(time.Second)

	q.fqMainMapping.Range(func(key string, value *queuemodels.QueuePublisher) bool {
		//for !value.IsClosed.Load() {
		//	runtime.Gosched()
		//}
		if !value.IsClosed.Load() {
			// TODO: close the file queue
			//if err := value.FileQueue.Close(); err != nil {
			//	//
			//}

			value.IsClosed.Store(true)
		}

		return true
	})

	q.fqDownstreamMapping.Range(func(key string, value *safe.GenericMap[*queuemodels.QueueSignaler]) bool {
		value.Range(func(key string, value *queuemodels.QueueSignaler) bool {
			//for !value.IsClosed.Load() {
			//	runtime.Gosched()
			//}
			if !value.IsClosed.Load() {
				// TODO: close the file queue
				//if err := value.FileQueue.Close(); err != nil {
				//	//
				//}

				value.IsClosed.Store(true)
			}

			return true
		})

		return true
	})

	q.ltngdbengine.Close()

	return nil
}

func (q *Queue) CreateQueue(
	ctx context.Context,
	queue *queuemodels.Queue,
) (qp *queuemodels.QueuePublisher, err error) {
	lockKey := queue.GetLockKey()
	q.opMtx.Lock(lockKey, struct{}{})
	defer q.opMtx.Unlock(lockKey)

	if err = queue.Validate(); err != nil {
		return nil, fmt.Errorf("error validating queue: %w", err)
	}

	if qp, err = q.createQueuePublisher(ctx, queue); err != nil {
		return nil, fmt.Errorf("failed to create queue publisher: %w", err)
	}

	if _, err = q.createQueueSignaler(ctx, queue); err != nil {
		return nil, fmt.Errorf("error creating queue: %w", err)
	}

	return
}

// CreateQueuePublisher creates a queue publisher.
// The queue publisher is the first point of contact with the ltng-queue.
// It is responsible for making sure the message was received and stored successfully
// to be later processed.
// Right first, process later, principle.
func (q *Queue) createQueuePublisher(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueuePublisher, error) {
	if _, err := q.createQueueStoreOnDB(ctx, queue); err != nil {
		return nil, fmt.Errorf("error creating queue store: %w", err)
	}

	fq, err := filequeuev1.New(ctx, queuemodels.PublishersSep+queue.Path, queue.Name)
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, fmt.Errorf("error creating queue: %w", err)
	}
	qp := &queuemodels.QueuePublisher{
		Queue:     queue,
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

	return qp, nil
}

func (q *Queue) createQueueSignaler(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	fq, err := filequeuev1.New(ctx, queuemodels.SignalersSep+queue.Path, queue.GetGroupName())
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, fmt.Errorf("error creating queue: %w", err)
	}

	lockKey := queue.GetLockKey()
	completeLockKey := queue.GetCompleteLockKey()

	fqdm, ok := q.fqDownstreamMapping.Get(lockKey)
	if !ok {
		fqdm = safe.NewGenericMap[*queuemodels.QueueSignaler]()
		q.fqDownstreamMapping.Set(lockKey, fqdm)
	}

	sinalTransmitterCount := queue.ConsumerCountLimit
	if sinalTransmitterCount == 0 {
		sinalTransmitterCount = signalTransmitterBufferSize
	}

	qs := &queuemodels.QueueSignaler{
		Queue:                  queue,
		FileQueue:              fq,
		SignalTransmissionRate: sinalTransmitterCount,
		SignalTransmitter:      make(chan struct{}, sinalTransmitterCount),
		FirstSent:              new(atomic.Bool),
		IsClosed:               new(atomic.Bool),
	}
	fqdm.Set(completeLockKey, qs)

	go q.consumerThread(q.ctx, qs)

	return qs, nil
}

func (q *Queue) getQueueSignaler(
	queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	lockKey := queue.GetLockKey()
	completeLockKey := queue.GetCompleteLockKey()

	fqdm, ok := q.fqDownstreamMapping.Get(lockKey)
	if !ok {
		fqdm = safe.NewGenericMap[*queuemodels.QueueSignaler]()
		q.fqDownstreamMapping.Set(lockKey, fqdm)
	}

	qs, ok := fqdm.Get(completeLockKey)
	if !ok {
		return nil, fmt.Errorf("error getting queue signaler")
	}

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

		qp, err = q.CreateQueue(ctx, queueInfo)
		if err != nil {
			log.Printf("error creating queue: %v", err)
		}

		return qp, nil
	}
	if qp.IsClosed.Load() {
		return nil, fmt.Errorf("queue is closed: %+v: %+v", queue, qp)
	}

	return qp, nil
}

func (q *Queue) Publish(ctx context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	// TODO: check whether or not these are necessary:
	// check retry count
	// create sent at
	// create received at

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
		event.Metadata.ReceivedAt = q.timer().Unix()
	}

	if event.Metadata.ReceivedAtList == nil {
		event.Metadata.ReceivedAtList = []int64{q.timer().Unix()}
	} else {
		event.Metadata.ReceivedAtList = append(
			event.Metadata.ReceivedAtList, q.timer().Unix())
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
	// TODO: change it to append unique. It only appends to the list if it does not exist on it yet.
	// so it traverses the existing list for checking item by item and then it appends if it does not exist.
	subscriptionQueueGroup.PublishList.Append(publisher)

	return nil
}

func (q *Queue) UnsubscribeFromQueue(
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

func (q *Queue) getQueueSubscribers(
	_ context.Context,
	queue *queuemodels.Queue,
) *queuemodels.QueueOrchestrator {
	lockKey := queue.GetLockKey()
	groupMapping, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		// TODO: make it debug log
		// log.Printf("no available queue subscriber for: %s", lockKey)

		return nil
	}

	completeLockKey := queue.GetCompleteLockKey()
	orchestrator, isOkay := groupMapping.Get(completeLockKey)
	if !isOkay {
		// TODO: make it debug log
		// log.Printf("no available group queue subscriber for: %s", completeLockKey)

		return nil
	}

	return orchestrator
}

func (q *Queue) publishToSubscribers(
	_ context.Context,
	event *queuemodels.Event,
	orchestrator *queuemodels.QueueOrchestrator,
) {
	switch orchestrator.Queue.QueueDistributionType {
	case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT:
		subscribers := orchestrator.PublishList.Get()
		for _, publisher := range subscribers {
			publisher.Sender <- event
		}
	case queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN:
		fallthrough
	default:
		orchestrator.PublishList.Next().Sender <- event
	}
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

		err := event.Validate()
		if err != nil {
			return fmt.Errorf("error validating event: %w: %w",
				err, errorsx.ErrUnRetryable)
		}

		// TODO: remove it when it is done!
		// fmt.Printf("event: %+v\n", event)

		lockKey := event.Queue.GetLockKey()
		fqdm, ok := q.fqDownstreamMapping.Get(lockKey)
		if !ok {
			return fmt.Errorf("queue not found for %s", lockKey)
		}

		if event.Queue.Group == nil {
			fqdm.Range(func(key string, subscriber *queuemodels.QueueSignaler) bool {
				if err = subscriber.FileQueue.WriteOnCursor(ctx, event); err != nil {
					fmt.Printf("error writing event to file queue %s - %v: %v", key, event, err)
				}

				return true
			})

			return nil
		}

		completeLockKey := event.Queue.GetCompleteLockKey()
		subscriber, isOk := fqdm.Get(completeLockKey)
		if !isOk {
			return fmt.Errorf("queue not found for %s", completeLockKey)
		}

		if err = subscriber.FileQueue.WriteOnCursor(ctx, event); err != nil {
			return fmt.Errorf("error writing event to file queue: %w: %w",
				err, errorsx.ErrRetryable)
		}

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
	eventChannel := make(chan *queuemodels.Event, queueSignaler.SignalTransmissionRate)
	go func() {
		ctxhandler.WithCancelLimit(ctx, queueSignaler.SignalTransmissionRate, func() error {
			// do not consume if there is no active subscribers
			orchestrator := q.getQueueSubscribers(ctx, queueSignaler.Queue)
			if orchestrator == nil {
				return nil
			}

			q.getQueueNextEvent(ctx, queueSignaler, eventChannel)

			return nil
		})
	}()

	go func() {
		ctxhandler.WithCancelLimit(ctx, queueSignaler.SignalTransmissionRate, func() error {
			q.handleEventLifecycle(ctx, queueSignaler, eventChannel)

			return nil
		})
	}()
}

func (q *Queue) getQueueNextEvent(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	eventChan chan *queuemodels.Event,
) {
	bs, err := queueSignaler.FileQueue.ReadFromCursorAndLockItWithoutTruncation(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}

		log.Printf("error reading from file queue: %s", err)
		return
	}

	var event queuemodels.Event
	if err = q.serializer.Deserialize(bs, &event); err != nil {
		log.Printf("error deserializing event from file queue: %s", err)
		return
	}

	if err = event.Validate(); err != nil {
		log.Printf("error validating event from file queue: %s", err)
		return
	}

	eventChan <- &event
}

func (q *Queue) handleEventLifecycle(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	eventChan chan *queuemodels.Event,
) {
	event, ok := <-eventChan
	if !ok {
		fmt.Printf("consumer channel is now closed: %v\n", ok)
		return
	}

	// TODO: remove it when it is done!
	//fmt.Printf("event id: %v\n", event.EventID)
	//fmt.Printf("event pulled from consumer thread: %+v\n", event)

	queueSignaler.SignalTransmitter <- struct{}{}
	defer func() {
		<-queueSignaler.SignalTransmitter
	}()

	orchestrator := q.getQueueSubscribers(ctx, event.Queue)
	if orchestrator == nil {
		log.Printf("no available queue subscriber for: %s: %v",
			event.Queue.Name, noAvailableSubscribers)

		return
	}

	et := q.createEventTracker(ctx, event, orchestrator)
	q.publishToSubscribers(ctx, event, orchestrator)

	q.handleEventWaiting(ctx, queueSignaler, et)
}

func (q *Queue) createEventTracker(
	_ context.Context,
	event *queuemodels.Event,
	orchestrator *queuemodels.QueueOrchestrator,
) *queuemodels.EventTracker {
	//subscriberCount := event.Queue.ConsumerCountLimit
	//ack := make(chan struct{}, subscriberCount)
	//nack := make(chan struct{}, subscriberCount)

	ack := make(chan struct{}, 1)
	nack := make(chan struct{}, 1)
	et := &queuemodels.EventTracker{
		EventID:   event.EventID,
		Event:     event,
		Ack:       ack,
		Nack:      nack,
		WasACKed:  new(atomic.Bool),
		WasNACKed: new(atomic.Bool),
	}
	q.eventMapTracker.Set(event.EventID, et)

	return et
}

func (q *Queue) handleEventWaiting(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	et *queuemodels.EventTracker,
) {
	eventIndex := []byte(et.EventID)

	ctxTimeout, cancel := context.WithTimeout(ctx, q.awaitTimeout)
	defer func() {
		cancel()
	}()

	select {
	case <-et.Ack:
		q.handleAck(ctxTimeout, queueSignaler, et.Event, eventIndex)
	case <-et.Nack:
		q.handleNack(ctxTimeout, queueSignaler, et.Event, eventIndex)
	case <-ctxTimeout.Done():
		q.handleAckNackTimeout(ctxTimeout, queueSignaler, et.Event, eventIndex)
	}
}

func (q *Queue) handleAck(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	event *queuemodels.Event,
	eventIndex []byte,
) {
	if err := queueSignaler.FileQueue.PopAndUnlockItFromIndex(ctx, eventIndex); err != nil {
		log.Printf("error popping queue: %v", err)
	}

	// TODO: debug log
	// log.Printf("event %v successfully ack'ed", event.EventID)

	q.eventMapTracker.Delete(event.EventID)
}

func (q *Queue) handleNack(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	event *queuemodels.Event,
	eventIndex []byte,
) {
	if err := queueSignaler.FileQueue.PopAndUnlockItFromIndex(ctx, eventIndex); err != nil {
		log.Printf("error popping queue: %v", err)
		return
	}

	// TODO: add retry count limit
	if event.Metadata.RetryCount >= q.retryCountLimit {
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
}

func (q *Queue) handleAckNackTimeout(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	event *queuemodels.Event,
	eventIndex []byte,
) {
	err := ctx.Err()
	if !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("event %v ack/nack timeout: %v", event.EventID, err)
		return
	}

	// TODO: make it debug log
	log.Printf("event context cancellation: %v",
		fmt.Errorf("event %v being cancelled: %v", event.EventID, err))

	q.handleNack(ctx, queueSignaler, event, eventIndex)
}

func (q *Queue) Ack(
	_ context.Context,
	event *queuemodels.Event,
) (*queuemodels.Event, error) {
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("error validating event: %v", err)
	}

	q.opMtx.Lock(event.EventID, struct{}{})
	defer q.opMtx.Unlock(event.EventID)

	//eventID := event.EventID
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message
	et, ok := q.eventMapTracker.Get(event.EventID)
	if !ok {
		return nil, fmt.Errorf(
			"no event tracker found for: %s - ack'ed",
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

	if et.WasNACKed.Load() {
		return et.Event, nil
	}

	et.Nack <- struct{}{}
	et.WasNACKed.Store(true)

	return et.Event, nil
}

// create the topic and subtopic - does nothing if it already exists
// create the subscriber - it can be for a group or the whole queue
// a file queue is consumed, and it posts to the other queues

// Secret Service - Your Service for Services's Secrets

// for the queue creation
// it creates the queue and the group queue (if it exists)
// it can have a routing group or not
// it does not define broadcasting rules; instead, subscribers do.
// subscribes can define the consumer policy (if it has not been set yet)
//
// consumer structure: map - map - queue-ref
// queue name - group name (should also contain a global one)
// how if there is no global consumer?
// how about there is a global and one or two group consumers?
//
// when an application tries to create a queue, a group queue, or
// whether it tries to subscribe to a queue or a group queue, it then creates it;
// if that is already created, do not error out.
// if a publisher tries to write to a non-existent queue, it should error out.
// if a subscriber tries to read from a non-existent queue, it should error out.
// if a subscriber tries to read from a non-existent group queue, it should error out.

// TODO: DO NOT USE READER POOLER HERE!!
// it needs to be async
// probably it is going to use the classic fq read and pop from index
//
// TODO: implement the downstream publishing logic

// TODO: check actual subscribers
// write to those file subscribers that are connected
// if no subscriber is connected, then do nothing until there is a subscriber;
// so keep checking for subscribers
// after the event is acked, write it to the ledger.
// if the event went through the max retry count; then write it to the DLQ ledger.
//
// apparently I do not need a publisher for the queue
// if the queue is not a group queue, then I do not need a publisher
// if the queue is a group queue, then I do not need a publisher for each group
// I only need a publisher for the complete lock key
//
// TODO: Eureka!!
// apparently I don't need a publisher and a queue signaler.
// just one of them...
// * pushes a message to the subscribed queues (all of them)
// messages to specific groups go to that group subscriber or
// a generic subscriber for that queue.
// a subscriber without a group receives all messages
// a subscriber with a group receives only messages from that group
//
// if the queue creation has a group, create only the group (complete lock key) file signaler
// if the queue creation does not have a group, create the lock key file signaler only
//
// TODO: what should we do with the event? Republish?
// and/or how about its internal reader cursor?
//
// TODO: how should we handle events when there are no subscribers?
// How would we revert the event to the queue? or its reader cursor?
// perhaps that if there is no subscriber, we should not consume at all! <--!!
//
// less effective would be to just pop the event from the queue; republish it
// and move on with the next event
