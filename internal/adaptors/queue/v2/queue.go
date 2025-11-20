package ltngqueueenginev2

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	serializer_models "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	ltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltngdbengine/v3"
	ltngdbmodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
)

const signalTransmitterBufferSize = 1 << 4

type Queue struct {
	ctx context.Context

	logger slogx.SLogger

	kvLock *syncx.KVLock
	mtx    *sync.RWMutex
	//op    *concurrent.OffThread

	awaitTimeout time.Duration

	serializer  serializer_models.Serializer
	fileManager *mmap.FileManager

	// this is the in-memory store information
	queueInfoStore *ltngdbmodelsv3.StoreInfo
	ltngdbengine   *ltngdbenginev3.LTNGEngine

	fqMainMapping       *syncx.GenericMap[*queuemodels.QueuePublisher]
	fqDownstreamMapping *syncx.GenericMap[*syncx.GenericMap[*queuemodels.QueueSignaler]]

	senderGroupMapping *syncx.GenericMap[*syncx.GenericMap[*queuemodels.QueueOrchestrator]]
	eventMapTracker    *syncx.GenericMap[*queuemodels.EventTracker]

	retryCountLimit uint64

	timer func() time.Time
}

func New(ctx context.Context, opts ...options.Option) (*Queue, error) {
	ltngdbengine, err := ltngdbenginev3.New(ctx)
	if err != nil {
		return nil, err
	}

	fm, err := mmap.NewFileManager(
		fileiomodels.GetFileQueueFilePath(
			fileiomodels.FileQueueMmapVersion,
			fileiomodels.GenericFileQueueFilePath,
			fileiomodels.GenericFileQueueFileName,
		))
	if err != nil {
		return nil, err
	}

	q := &Queue{
		ctx: ctx,

		logger: slogx.New(),

		kvLock: syncx.NewKVLock(),
		mtx:    &sync.RWMutex{},
		//op:    concurrent.New("ltng-queue"),

		serializer:  serializer.NewRawBinarySerializer(),
		fileManager: fm,

		ltngdbengine: ltngdbengine,

		fqMainMapping:       syncx.NewGenericMap[*queuemodels.QueuePublisher](),
		fqDownstreamMapping: syncx.NewGenericMap[*syncx.GenericMap[*queuemodels.QueueSignaler]](),

		senderGroupMapping: syncx.NewGenericMap[*syncx.GenericMap[*queuemodels.QueueOrchestrator]](),
		eventMapTracker:    syncx.NewGenericMap[*queuemodels.EventTracker](),

		retryCountLimit: 5,
		awaitTimeout:    time.Second * 5,

		timer: time.Now,
	}

	queueStoreInfo, err := q.runQueueMigration(ctx)
	if err != nil {
		return nil, errorsx.Wrap(err, "failed to run migration")
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

	q.fqDownstreamMapping.Range(func(key string, value *syncx.GenericMap[*queuemodels.QueueSignaler]) bool {
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
	q.kvLock.Lock(lockKey, struct{}{})
	defer q.kvLock.Unlock(lockKey)

	if err = queue.Validate(); err != nil {
		return nil, errorsx.Wrap(err, "error validating queue")
	}

	if qp, err = q.createQueuePublisher(ctx, queue); err != nil {
		return nil, errorsx.Wrap(err, "failed to create queue publisher")
	}

	if _, err = q.createQueueSignaler(ctx, queue); err != nil {
		return nil, errorsx.Wrap(err, "error creating queue")
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
		return nil, errorsx.Wrap(err, "error creating queue store")
	}

	fq, err := mmap.NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		filepath.Join(queuemodels.Publishers, queue.Path), queue.Name))
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, errorsx.Wrap(err, "error creating queue")
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
		return nil, errorsx.Wrap(err, "error persisting queue")
	}

	go q.readerPuller(q.ctx, qp)

	return qp, nil
}

func (q *Queue) createQueueSignaler(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueueSignaler, error) {
	fq, err := mmap.NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		filepath.Join(queuemodels.Signalers, queue.Path), queue.GetGroupName()))
	if err != nil { //  && !strings.Contains(err.Error(), "file already exist")
		return nil, errorsx.Wrap(err, "error creating queue")
	}

	lockKey := queue.GetLockKey()
	completeLockKey := queue.GetCompleteLockKey()

	fqdm, ok := q.fqDownstreamMapping.Get(lockKey)
	if !ok {
		fqdm = syncx.NewGenericMap[*queuemodels.QueueSignaler]()
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
		fqdm = syncx.NewGenericMap[*queuemodels.QueueSignaler]()
		q.fqDownstreamMapping.Set(lockKey, fqdm)
	}

	qs, ok := fqdm.Get(completeLockKey)
	if !ok {
		return nil, errorsx.New("error getting queue signaler")
	}

	return qs, nil
}

// createQueueStoreOnDB is responsible to hold the queue messages for historical reasons.
// with that we can query the messages from the queue store.
func (q *Queue) createQueueStoreOnDB(
	ctx context.Context, queue *queuemodels.Queue,
) (*ltngdbmodelsv3.StoreInfo, error) {
	info := &ltngdbmodelsv3.StoreInfo{
		Name:         queue.Name,
		Path:         queue.Path,
		CreatedAt:    time.Now().UTC().Unix(),
		LastOpenedAt: time.Now().UTC().Unix(),
	}
	if _, err := q.ltngdbengine.CreateStore(ctx, info); err != nil {
		return nil, errorsx.Wrap(err, "error creating queue")
	}

	return info, nil
}

func (q *Queue) saveQueueReferenceOnDB(
	ctx context.Context, queue *queuemodels.Queue,
) (*ltngdbmodelsv3.Item, error) {
	lockKey := queue.GetLockKey()
	bs, err := q.serializer.Serialize(queue)
	if err != nil {
		q.logger.Debug(ctx, "error serializing queue", "error", err)
	}

	dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
	item := &ltngdbmodelsv3.Item{
		Key:   []byte(lockKey),
		Value: bs,
	}
	opts := &ltngdbmodelsv3.IndexOpts{
		HasIdx: false,
	}

	if queue.Group != nil {
		completeLockKey := queue.GetCompleteLockKey()
		opts = &ltngdbmodelsv3.IndexOpts{
			HasIdx:       true,
			ParentKey:    []byte(lockKey),
			IndexingKeys: [][]byte{[]byte(lockKey), []byte(completeLockKey)},
		}
	}

	item, err = q.ltngdbengine.UpsertItem(ctx, dbMetaInfo, item, opts)
	if err != nil && !strings.Contains(err.Error(), "file already exist") {
		q.logger.Debug(ctx, "error creating queue item", "error", err)
	}

	return item, nil
}

func (q *Queue) deleteQueueReferenceFromDB(
	ctx context.Context, queue *queuemodels.Queue,
) error {
	lockKey := queue.GetLockKey()
	dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
	item := &ltngdbmodelsv3.Item{
		Key: []byte(lockKey),
	}
	opts := &ltngdbmodelsv3.IndexOpts{
		HasIdx: false,
	}
	//if queue.Group != nil {
	//	completeLockKey := queue.GetCompleteLockKey()
	//	opts = &ltngdbmodelsv3.IndexOpts{
	//		HasIdx:       true,
	//		IndexingKeys: [][]byte{[]byte(lockKey), []byte(completeLockKey)},
	//		IndexProperties: ltngdbmodelsv3.IndexProperties{
	//			IndexDeletionBehaviour: ltngdbmodelsv3.CascadeByIdx,
	//		},
	//	}
	//}
	_, err := q.ltngdbengine.DeleteItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		q.logger.Debug(ctx, "error loading queue item", "error", err)
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
	item := &ltngdbmodelsv3.Item{
		Key: []byte(lockKey),
	}
	completeLockKey := queue.GetCompleteLockKey()
	opts := &ltngdbmodelsv3.IndexOpts{
		HasIdx:       true,
		IndexingKeys: [][]byte{[]byte(completeLockKey)},
		IndexProperties: ltngdbmodelsv3.IndexProperties{
			IndexDeletionBehaviour: ltngdbmodelsv3.IndexOnly,
		},
	}
	_, err := q.ltngdbengine.DeleteItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		q.logger.Debug(ctx, "error loading queue item", "error", err)
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
	return errorsx.New("not implemented")
}

func (q *Queue) getQueuePublisher(
	ctx context.Context, queue *queuemodels.Queue,
) (*queuemodels.QueuePublisher, error) {
	lockKey := queue.GetLockKey()
	qp, ok := q.fqMainMapping.Get(lockKey)
	if !ok {
		dbMetaInfo := q.queueInfoStore.ManagerStoreMetaInfo()
		item := &ltngdbmodelsv3.Item{
			Key: []byte(lockKey),
		}
		opts := &ltngdbmodelsv3.IndexOpts{
			HasIdx: false,
		}
		queueItem, err := q.ltngdbengine.LoadItem(ctx, dbMetaInfo, item, opts)
		if err != nil {
			q.logger.Debug(ctx, "error loading queue item", "error", err)
		}

		var queueInfo *queuemodels.Queue
		if err = q.serializer.Deserialize(queueItem.Value, &queueInfo); err != nil {
			q.logger.Debug(ctx, "error deserializing queue item", "error", err)
		}

		qp, err = q.CreateQueue(ctx, queueInfo)
		if err != nil {
			q.logger.Debug(ctx, "error creating queue", "error", err)
		}

		return qp, nil
	}
	if qp.IsClosed.Load() {
		return nil, errorsx.Errorf("queue is closed: %+v", queue)
	}

	return qp, nil
}

func (q *Queue) Publish(ctx context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	// TODO: check whether or not these are necessary:
	// check retry count
	// create sent at
	// create received at

	if err := event.Validate(); err != nil {
		return nil, errorsx.Wrap(err, "error validating event")
	}

	if event.EventID == "" {
		newEventUUID, err := uuid.NewV7()
		if err != nil {
			return nil, errorsx.Wrap(err, "error generating uuid")
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
		return nil, errorsx.Wrap(err, "error getting queue")
	}

	if _, err = qp.FileQueue.Write(event); err != nil {
		return nil, errorsx.Wrap(err, "error writing event")
	}

	return event, nil
}

func (q *Queue) SubscribeToQueue(
	_ context.Context,
	queue *queuemodels.Queue,
	publisher *queuemodels.Publisher,
) error {
	lockKey := queue.GetLockKey()
	q.kvLock.Lock(lockKey, struct{}{})
	defer q.kvLock.Unlock(lockKey)
	completeLockKey := queue.GetCompleteLockKey()

	subscriptionQueue, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		subscriptionQueue = syncx.NewGenericMap[*queuemodels.QueueOrchestrator]()
		q.senderGroupMapping.Set(lockKey, subscriptionQueue)
	}

	subscriptionQueueGroup, isOk := subscriptionQueue.Get(completeLockKey)
	if !isOk {
		subscriptionQueueGroup = &queuemodels.QueueOrchestrator{
			Queue: queue,
			PublishList: syncx.NewTicketStorageLoop[*queuemodels.Publisher](
				syncx.WithTicketStorageSize[*queuemodels.Publisher](0),
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
	q.kvLock.Lock(lockKey, struct{}{})
	defer q.kvLock.Unlock(lockKey)
	completeLockKey := queue.GetCompleteLockKey()

	subscriptionQueue, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		return errorsx.Errorf("queue not found for %s", lockKey)
	}
	subscriptionGroupQueue, isOk := subscriptionQueue.Get(completeLockKey)
	if !isOk {
		return errorsx.Errorf("group queue not found for %s", lockKey)
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
	ctx context.Context,
	queue *queuemodels.Queue,
) *queuemodels.QueueOrchestrator {
	lockKey := queue.GetLockKey()
	groupMapping, ok := q.senderGroupMapping.Get(lockKey)
	if !ok {
		q.logger.Debug(ctx, "no available queue subscriber for key", "key", lockKey)

		return nil
	}

	completeLockKey := queue.GetCompleteLockKey()
	orchestrator, isOkay := groupMapping.Get(completeLockKey)
	if !isOkay {
		q.logger.Debug(ctx, "no available group queue subscriber for key", "key", completeLockKey)

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

func (q *Queue) readerPuller(
	ctx context.Context,
	queuePublisher *queuemodels.QueuePublisher,
) {
	loop.Run(ctx, func() error {
		bs, err := queuePublisher.FileQueue.Read()
		if err != nil {
			return errorsx.Wrap(err, "error reading from queue")
		}

		var event queuemodels.Event
		if err := q.serializer.Deserialize(bs, &event); err != nil {
			return errorsx.Wrap(err, "error deserializing event from file queue").(*errorsx.Error).
				WithNonRetriable()
		}

		err = event.Validate()
		if err != nil {
			return errorsx.Wrap(err, "error validating event").(*errorsx.Error).
				WithNonRetriable()
		}

		lockKey := event.Queue.GetLockKey()
		fqdm, ok := q.fqDownstreamMapping.Get(lockKey)
		if !ok {
			return errorsx.Errorf("queue not found for %s", lockKey)
		}

		if event.Queue.Group == nil {
			fqdm.Range(func(key string, subscriber *queuemodels.QueueSignaler) bool {
				if _, err = subscriber.FileQueue.Write(event); err != nil {
					q.logger.Debug(ctx, "error writing event to file queue",
						"key", key, "event", event, "error", err)
				}

				return true
			})

			return nil
		}

		completeLockKey := event.Queue.GetCompleteLockKey()
		subscriber, isOk := fqdm.Get(completeLockKey)
		if !isOk {
			return errorsx.Errorf("queue not found for %s", completeLockKey).
				WithRetriable()
		}

		if _, err = subscriber.FileQueue.Write(event); err != nil {
			return errorsx.Wrap(err, "error writing event to file queue").(*errorsx.Error).
				WithRetriable()
		}

		_, err = queuePublisher.FileQueue.DeleteByKey(ctx, bs)
		if err != nil {
			return errorsx.Wrap(err, "error deleting event from file queue").(*errorsx.Error).
				WithRetriable()
		}

		return nil
	})

	//err := queuePublisher.FileQueue.ReaderPooler(ctx, func(ctx context.Context, bs []byte) error {
	//	var event queuemodels.Event
	//	if err := q.serializer.Deserialize(bs, &event); err != nil {
	//		return errorsx.Wrap(err, "error deserializing event from file queue").(*errorsx.Error).
	//			WithNonRetriable()
	//	}
	//
	//	err := event.Validate()
	//	if err != nil {
	//		return errorsx.Wrap(err, "error validating event").(*errorsx.Error).WithNonRetriable()
	//	}
	//
	//	lockKey := event.Queue.GetLockKey()
	//	fqdm, ok := q.fqDownstreamMapping.Get(lockKey)
	//	if !ok {
	//		return errorsx.Errorf("queue not found for %s", lockKey)
	//	}
	//
	//	if event.Queue.Group == nil {
	//		fqdm.Range(func(key string, subscriber *queuemodels.QueueSignaler) bool {
	//			if _, err = subscriber.FileQueue.Write(event); err != nil {
	//				q.logger.Debug(ctx, "error writing event to file queue",
	//					"key", key, "event", event, "error", err)
	//			}
	//
	//			return true
	//		})
	//
	//		return nil
	//	}
	//
	//	completeLockKey := event.Queue.GetCompleteLockKey()
	//	subscriber, isOk := fqdm.Get(completeLockKey)
	//	if !isOk {
	//		return errorsx.Errorf("queue not found for %s", completeLockKey)
	//	}
	//
	//	if _, err = subscriber.FileQueue.Write(event); err != nil {
	//		return errorsx.Wrap(err, "error writing event to file queue").(*errorsx.Error).
	//			WithRetriable()
	//	}
	//
	//	return nil
	//})
	//if err != nil {
	//	q.logger.Debug(ctx, "error reading/closing from queue", "error", err)
	//}

	queuePublisher.IsClosed.Store(true)
}

func (q *Queue) consumerThread(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
) {
	eventChannel := make(chan *queuemodels.Event, queueSignaler.SignalTransmissionRate)
	go func() {
		loop.RunWithLimit(ctx, queueSignaler.SignalTransmissionRate, func() error {
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
		loop.RunWithLimit(ctx, queueSignaler.SignalTransmissionRate, func() error {
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
	bs, err := queueSignaler.FileQueue.ReadLock()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}

		q.logger.Debug(ctx, "error reading from file queue", "error", err)

		return
	}

	var event queuemodels.Event
	if err = q.serializer.Deserialize(bs, &event); err != nil {
		q.logger.Debug(ctx, "error deserializing event from file queue", "error", err)
		return
	}

	if err = event.Validate(); err != nil {
		q.logger.Debug(ctx, "error validating event from file queue", "error", err)
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
		q.logger.Debug(ctx, "consumer channel is now closed", "is_closed", ok)
		return
	}

	queueSignaler.SignalTransmitter <- struct{}{}
	defer func() {
		<-queueSignaler.SignalTransmitter
	}()

	orchestrator := q.getQueueSubscribers(ctx, event.Queue)
	if orchestrator == nil {
		q.logger.Debug(ctx, "no available queue subscriber for", "queue_name", event.Queue.Name)

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
	if orchestrator.Queue.AckPolicy == queuemodels.ACKPolicy_ACK_POLICY_ALL {
		subscriberCount := len(orchestrator.PublishList.Get())
		ack = make(chan struct{}, subscriberCount)
		nack = make(chan struct{}, subscriberCount)
	}

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
	if _, err := queueSignaler.FileQueue.DeleteByKeyUnlock(ctx, eventIndex); err != nil {
		q.logger.Debug(ctx, "error popping queue", "error", err)
	}

	q.eventMapTracker.Delete(event.EventID)
}

func (q *Queue) handleNack(
	ctx context.Context,
	queueSignaler *queuemodels.QueueSignaler,
	event *queuemodels.Event,
	eventIndex []byte,
) {
	if _, err := queueSignaler.FileQueue.DeleteByKeyUnlock(ctx, eventIndex); err != nil {
		q.logger.Debug(ctx, "error popping queue", "error", err)
		return
	}

	// TODO: add retry count limit
	if event.Metadata.RetryCount >= q.retryCountLimit {
		// TODO: log and send it to the DLQ if existent?
		return
	}
	event.Metadata.RetryCount++

	if _, err := q.Publish(ctx, event); err != nil {
		q.logger.Debug(ctx, "error re-publishing nacked event", "error", err)
	}

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
		q.logger.Debug(ctx, "event ack/nack timeout", "event_id", event.EventID, "error", err)
		return
	}

	q.logger.Debug(ctx, "context cancellation", "error",
		errorsx.Wrapf(err, "event %v being cancelled", event.EventID))

	q.handleNack(ctx, queueSignaler, event, eventIndex)
}

func (q *Queue) Ack(
	_ context.Context,
	event *queuemodels.Event,
) (*queuemodels.Event, error) {
	if err := event.Validate(); err != nil {
		return nil, errorsx.Wrap(err, "error validating event")
	}

	q.kvLock.Lock(event.EventID, struct{}{})
	defer q.kvLock.Unlock(event.EventID)

	//eventID := event.EventID
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message
	et, ok := q.eventMapTracker.Get(event.EventID)
	if !ok {
		return nil, errorsx.Errorf("no event tracker found for: %s - ack'ed", event.EventID)
	}

	if et.WasACKed.Load() {
		return et.Event, nil
	}

	et.Ack <- struct{}{}
	et.WasACKed.Store(true)

	return et.Event, nil
}

func (q *Queue) Nack(_ context.Context, event *queuemodels.Event) (*queuemodels.Event, error) {
	q.kvLock.Lock(event.EventID, struct{}{})
	defer q.kvLock.Unlock(event.EventID)

	//eventID := event.EventID
	// TODO: republish
	// TODO: remove from the timed-base-store
	// TODO: send to sigchannel to fetch next event message

	et, ok := q.eventMapTracker.Get(event.EventID)
	if !ok {
		return nil, errorsx.Errorf("no event mapper found for %s: failed to ack event message", event.EventID)
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
