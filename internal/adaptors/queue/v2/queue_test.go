package ltngqueueenginev2

import (
	"context"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/random"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

const (
	ltngDBBasePath        = ".ltngdb"
	ltngFileQueueBasePath = ".ltngfq"
)

type (
	GenericTestData struct {
		FieldString string
		FieldInt    int
		FieldBool   bool
	}
)

func TestQueue(t *testing.T) {
	t.Run("queue instantiation", func(t *testing.T) {
		Test_DeleteTestFileQueue(t)

		ctx := context.Background()

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("queue creation", func(t *testing.T) {
		Test_DeleteTestFileQueue(t)

		ctx := context.Background()

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",
		}
		_, err = ltngqueue.CreateQueue(ctx, queue)
		require.NoError(t, err)

		err = ltngqueue.Close()
		require.NoError(t, err)
	})
}

func TestQueue_CreateQueue(t *testing.T) {
	t.Run("create queue", func(t *testing.T) {
		Test_DeleteTestFileQueue(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		qp, err := ltngqueue.CreateQueue(ctx, queue)
		assert.NoError(t, err)
		assert.Equal(t, queue, qp.Queue)

		gqp, err := ltngqueue.getQueuePublisher(ctx, qp.Queue)
		assert.NoError(t, err)
		assert.EqualValues(t, qp, gqp)

		lockKey := queue.GetLockKey()
		completeLockKey := queue.GetCompleteLockKey()

		fqdm, ok := ltngqueue.fqDownstreamMapping.Get(lockKey)
		assert.True(t, ok)
		qs, ok := fqdm.Get(completeLockKey)
		assert.True(t, ok)
		assert.Equal(t, queue, qs.Queue)

		time.Sleep(time.Millisecond * 5)

		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
	})
}

func TestQueue_SubscribeToQueue(t *testing.T) {
	t.Run("subscribe to queue", func(t *testing.T) {
		Test_DeleteTestFileQueue(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}

		nodeUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

		publisher := &queuemodels.Publisher{
			NodeID: nodeID,
			Sender: make(chan *queuemodels.Event, 10),
		}

		err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
		assert.NoError(t, err)

		lockKey := queue.GetLockKey()
		completeLockKey := queue.GetCompleteLockKey()

		subscriptionQueue, ok := ltngqueue.senderGroupMapping.Get(lockKey)
		assert.True(t, ok)
		subscriptionQueueGroup, ok := subscriptionQueue.Get(completeLockKey)
		assert.True(t, ok)
		assert.Equal(t, queue, subscriptionQueueGroup.Queue)
	})
}

func TestQueue_Publish(t *testing.T) {
	testCases := map[string]struct {
		eventCount int
	}{
		"single event": {
			eventCount: 1,
		},
		"10 events": {
			eventCount: 10,
		},
		"50 events": {
			eventCount: 50,
		},
		"100 events": {
			eventCount: 100,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			Test_DeleteTestFileQueue(t)

			ctx, cancel := context.WithCancel(context.Background())

			ltngqueue, err := New(ctx)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name: "test-queue",
				Path: "test/queue",

				// consumerCountLimit has no effect if there are no subscribers
				//ConsumerCountLimit: testCase.consumerCountLimit,
			}
			_, err = ltngqueue.CreateQueue(ctx, queue)
			require.NoError(t, err)

			// generate & publish events
			events := generateEventList(t, ltngqueue.serializer, queue, testCase.eventCount)
			for i := 0; i < testCase.eventCount; i++ {
				event := events[i]
				e, err := ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				require.EqualValues(t, event, e)
			}

			qs, err := ltngqueue.getQueueSignaler(queue)
			assert.Nil(t, err)

			// pull & assert events - order should be preserved
			count := new(atomic.Uint64)
			go func() {
				loop.Run(ctx, func() error {
					bs, err := qs.FileQueue.Read()
					if err != io.EOF {
						assert.NoError(t, err)
					}

					if len(bs) == 0 {
						return nil
					}

					var event queuemodels.Event
					err = ltngqueue.serializer.Deserialize(bs, &event)
					assert.NoError(t, err)
					err = event.Validate()
					assert.NoError(t, err)

					assert.EqualValues(t, events[count.Load()], &event)
					count.Add(1)

					return nil
				})

				//err := qs.FileQueue.ReaderPooler(ctx, func(ctx context.Context, bs []byte) error {
				//	if len(bs) == 0 {
				//		return nil
				//	}
				//
				//	var event queuemodels.Event
				//	err = ltngqueue.serializer.Deserialize(bs, &event)
				//	assert.NoError(t, err)
				//	err = event.Validate()
				//	assert.NoError(t, err)
				//
				//	assert.EqualValues(t, events[count.Load()], &event)
				//	count.Add(1)
				//	return nil
				//})
				//assert.NoError(t, err)
			}()

			for count.Load() != uint64(testCase.eventCount) {
				runtime.Gosched()
			}
			t.Log(count.Load())

			time.Sleep(time.Millisecond * 5)

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)
		})
	}
}

func TestQueue_PublishConcurrently(t *testing.T) {
	testCases := map[string]struct {
		eventCount int
	}{
		"single event": {
			eventCount: 1,
		},
		"10 events": {
			eventCount: 10,
		},
		//"50 events": {
		//	eventCount: 50,
		//},
		//"100 events": {
		//	eventCount: 100,
		//},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			Test_DeleteTestFileQueue(t)

			ctx, cancel := context.WithCancel(context.Background())

			ltngqueue, err := New(ctx)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name: "test-queue",
				Path: "test/queue",

				// consumerCountLimit has no effect if there are no subscribers
				//ConsumerCountLimit: testCase.consumerCountLimit,
			}
			_, err = ltngqueue.CreateQueue(ctx, queue)
			require.NoError(t, err)

			op := syncx.NewThreadOperator("publisher", syncx.WithThreadLimit(64))

			// generate & publish events
			events := generateEventList(t, ltngqueue.serializer, queue, testCase.eventCount)
			eventMap := eventListToEventMap(events)
			eventMapCheck := make(map[string]*queuemodels.Event)
			for i := 0; i < testCase.eventCount; i++ {
				op.Op(func() {
					event := events[i]
					e, err := ltngqueue.Publish(ctx, event)
					require.NoError(t, err)
					require.EqualValues(t, event, e)
				})
			}

			qs, err := ltngqueue.getQueueSignaler(queue)
			assert.Nil(t, err)

			// pull & assert events - order should be preserved
			count := new(atomic.Uint64)
			go func() {
				loop.Run(ctx, func() error {
					bs, err := qs.FileQueue.Read()
					if err != io.EOF {
						assert.NoError(t, err)
					}

					if len(bs) == 0 {
						return nil
					}

					var event queuemodels.Event
					err = ltngqueue.serializer.Deserialize(bs, &event)
					assert.NoError(t, err)
					err = event.Validate()
					assert.NoError(t, err)

					e, ok := eventMap[event.EventID]
					assert.True(t, ok)
					assert.EqualValues(t, e, &event)

					_, ok = eventMapCheck[event.EventID]
					assert.False(t, ok)
					eventMapCheck[event.EventID] = e

					count.Add(1)
					return nil
				})

				//err := qs.FileQueue.ReaderPooler(ctx, func(ctx context.Context, bs []byte) error {
				//	if len(bs) == 0 {
				//		return nil
				//	}
				//
				//	var event queuemodels.Event
				//	err = ltngqueue.serializer.Deserialize(bs, &event)
				//	assert.NoError(t, err)
				//	err = event.Validate()
				//	assert.NoError(t, err)
				//
				//	e, ok := eventMap[event.EventID]
				//	assert.True(t, ok)
				//	assert.EqualValues(t, e, &event)
				//
				//	_, ok = eventMapCheck[event.EventID]
				//	assert.False(t, ok)
				//	eventMapCheck[event.EventID] = e
				//
				//	count.Add(1)
				//	return nil
				//})
				//assert.NoError(t, err)
			}()

			for count.Load() != uint64(testCase.eventCount) {
				runtime.Gosched()
			}
			t.Log(count.Load())

			time.Sleep(time.Millisecond * 5)

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)
		})
	}
}

func TestQueue_Consume(t *testing.T) {
	testCases := map[string]struct {
		eventCount         int
		consumerCountLimit uint32
	}{
		"single event": {
			eventCount:         1,
			consumerCountLimit: 1,
		},
		"10 events": {
			eventCount:         10,
			consumerCountLimit: 1,
		},
		"50 events": {
			eventCount:         50,
			consumerCountLimit: 1,
		},
		"100 events": {
			eventCount:         100,
			consumerCountLimit: 1,
		},
		"1_000 events": {
			eventCount:         1_000,
			consumerCountLimit: 1,
		},
		"5_000 events": {
			eventCount:         5_000,
			consumerCountLimit: 1,
		},
		"10_000 events": {
			eventCount:         10_000,
			consumerCountLimit: 1,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			Test_DeleteTestFileQueue(t)

			ctx, cancel := context.WithCancel(context.Background())

			ltngqueue, err := New(ctx,
				WithTimeout(time.Millisecond),
			)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name: "test-queue",
				Path: "test/queue",

				ConsumerCountLimit: testCase.consumerCountLimit,
			}
			_, err = ltngqueue.CreateQueue(ctx, queue)
			require.NoError(t, err)

			// generate & publish events
			// publishing in single thread mode in order to assert and regress ordering
			events := generateEventList(t, ltngqueue.serializer, queue, testCase.eventCount)
			for i := 0; i < testCase.eventCount; i++ {
				event := events[i]
				e, err := ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				require.EqualValues(t, event, e)
			}
			require.Len(t, events, testCase.eventCount)

			nodeUUID, err := uuid.NewRandom()
			require.NoError(t, err)
			nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

			receiver := make(chan *queuemodels.Event, 1)
			publisher := &queuemodels.Publisher{
				NodeID: nodeID,
				Sender: receiver,
			}
			err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
			require.NoError(t, err)

			count := new(atomic.Uint64)
			var consumedEvents []*queuemodels.Event
			go func() {
				for event := range receiver {
					consumedEvents = append(consumedEvents, event)
					_, err := ltngqueue.Ack(ctx, event)
					assert.NoError(t, err)
					expectedEvent := events[count.Load()]
					assert.EqualValues(t, expectedEvent, event)
					count.Add(1)
				}
			}()

			for count.Load() != uint64(testCase.eventCount) {
				runtime.Gosched()
			}
			t.Log(count.Load())
			if count.Load() < 50 {
				t.Log("events")
				for _, event := range events {
					t.Log(event)
				}
				t.Log("consumedEvents")
				for _, event := range consumedEvents {
					t.Log(event)
				}
			}

			time.Sleep(time.Millisecond * 5)

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)
		})
	}
}

type eventHandle func(
	ltngqueue *Queue, event *queuemodels.Event,
) (*queuemodels.Event, error)

type handlingEventType string

const (
	ackHET     handlingEventType = "ack"
	nackHET    handlingEventType = "nack"
	timeoutHET handlingEventType = "timeout"
)

var eventHandleTypes = map[handlingEventType]eventHandle{
	ackHET: func(ltngqueue *Queue, event *queuemodels.Event) (*queuemodels.Event, error) {
		return ltngqueue.Ack(context.Background(), event)
	},
	nackHET: func(ltngqueue *Queue, event *queuemodels.Event) (*queuemodels.Event, error) {
		return ltngqueue.Nack(context.Background(), event)
	},
	timeoutHET: func(ltngqueue *Queue, event *queuemodels.Event) (*queuemodels.Event, error) {
		return event, nil
	},
}

type consumerTestCase struct {
	eventCount         int
	consumerCountLimit uint32
	subscriberCount    int
}

func TestQueue_ConsumeConcurrently(t *testing.T) {
	testCases := map[string]consumerTestCase{
		"single event - 1 consumer - 1 subscriber": {
			eventCount:         1,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"10 events - 1 consumer - 1 subscriber": {
			eventCount:         10,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"50 events - 1 consumer - 1 subscriber": {
			eventCount:         50,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"100 events - 1 consumer - 1 subscriber": {
			eventCount:         100,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"1 event - 5 consumers - 1 subscriber": {
			eventCount:         1,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"5 events - 5 consumers - 1 subscriber": {
			eventCount:         5,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"10 events - 5 consumers - 1 subscriber": {
			eventCount:         10,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"50 events - 5 consumers - 1 subscriber": {
			eventCount:         50,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"100 events - 5 consumers - 1 subscriber": {
			eventCount:         100,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"1 event - 5 consumers - 5 subscribers": {
			eventCount:         1,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"10 events - 5 consumers - 5 subscribers": {
			eventCount:         10,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"50 events - 5 consumers - 5 subscribers": {
			eventCount:         50,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"100 events - 5 consumers - 5 subscribers": {
			eventCount:         100,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"1 event - 1 consumer - 5 subscribers": {
			eventCount:         1,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
		"10 events - 1 consumer - 5 subscribers": {
			eventCount:         10,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
		"50 events - 1 consumer - 5 subscribers": {
			eventCount:         50,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
		"100 events - 1 consumer - 5 subscribers": {
			eventCount:         100,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testConsumerConcurrently(
				t, testCase, eventHandleTypes[ackHET],
				WithTimeout(time.Millisecond*500),
			)
		})
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testConsumerConcurrently(
				t, testCase, eventHandleTypes[nackHET],
				WithTimeout(time.Millisecond*500),
				WithRetryCountLimit(0),
			)
		})
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testConsumerConcurrently(
				t, testCase, eventHandleTypes[timeoutHET],
				WithTimeout(time.Millisecond*1),
				WithRetryCountLimit(0),
			)
		})
	}
}

func testConsumerConcurrently(
	t *testing.T,
	testCase consumerTestCase,
	handleEvent eventHandle,
	opts ...options.Option,
) {
	Test_DeleteTestFileQueue(t)

	ctx, cancel := context.WithCancel(context.Background())

	ltngqueue, err := New(ctx, opts...)
	require.NoError(t, err)

	queue := &queuemodels.Queue{
		Name: "test-queue",
		Path: "test/queue",

		// consumerCountLimit has no effect if there are no subscribers
		ConsumerCountLimit: testCase.consumerCountLimit,
	}
	_, err = ltngqueue.CreateQueue(ctx, queue)
	require.NoError(t, err)

	op := syncx.NewThreadOperator("publisher", syncx.WithThreadLimit(64))

	// generate & publish events
	events := generateEventList(t, ltngqueue.serializer, queue, testCase.eventCount)
	eventMap := eventListToSafeEventMap(events)
	eventMapCheck := syncx.NewGenericMap[*queuemodels.Event]()
	for i := 0; i < testCase.eventCount; i++ {
		op.Op(func() {
			event := events[i]
			e, err := ltngqueue.Publish(ctx, event)
			require.NoError(t, err)
			require.EqualValues(t, event, e)
		})
	}

	op.Wait()

	nodeIdList := make([]string, testCase.subscriberCount)
	receiverList := make([]chan *queuemodels.Event, testCase.subscriberCount)
	for i := 0; i < testCase.subscriberCount; i++ {
		nodeUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

		receiver := make(chan *queuemodels.Event, 1)
		publisher := &queuemodels.Publisher{
			NodeID: nodeID,
			Sender: receiver,
		}
		err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
		require.NoError(t, err)

		nodeIdList[i] = nodeID
		receiverList[i] = receiver
	}

	count := new(atomic.Uint64)
	consumedEvents := syncx.NewTicketStorage[*queuemodels.Event](testCase.eventCount)
	go func() {
		for _, receiver := range receiverList {
			go func() {
				for event := range receiver {
					handledEvent, err := handleEvent(ltngqueue, event)
					assert.NoError(t, err)
					assert.EqualValues(t, event, handledEvent)

					consumedEvents.Put(event)

					e, ok := eventMap.Get(event.EventID)
					assert.True(t, ok)
					assert.EqualValues(t, e, event)

					e, ok = eventMapCheck.Get(event.EventID)
					assert.False(t, ok)
					eventMapCheck.Set(event.EventID, event)

					count.Add(1)
				}
			}()
		}
	}()

	for count.Load() != uint64(testCase.eventCount) {
		runtime.Gosched()
	}
	t.Log(count.Load())
	//t.Log("events")
	//for _, event := range events {
	//	t.Log(event)
	//}
	//t.Log("consumedEvents")
	//for _, event := range consumedEvents.Get() {
	//	t.Log(event)
	//}

	for _, nodeID := range nodeIdList {
		err = ltngqueue.UnsubscribeFromQueue(ctx, queue,
			&queuemodels.Publisher{
				NodeID: nodeID,
			})
		require.NoError(t, err)
	}

	time.Sleep(time.Millisecond * 5)

	cancel()
	err = ltngqueue.Close()
	require.NoError(t, err)
}

func TestQueue_RetryBehaviour(t *testing.T) {
	testCases := map[string]consumerTestCase{
		"single event - 1 consumer - 1 subscriber": {
			eventCount:         1,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"10 events - 1 consumer - 1 subscriber": {
			eventCount:         10,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"50 events - 1 consumer - 1 subscriber": {
			eventCount:         50,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"100 events - 1 consumer - 1 subscriber": {
			eventCount:         100,
			consumerCountLimit: 1,
			subscriberCount:    1,
		},
		"1 event - 5 consumers - 1 subscriber": {
			eventCount:         1,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"5 events - 5 consumers - 1 subscriber": {
			eventCount:         5,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"10 events - 5 consumers - 1 subscriber": {
			eventCount:         10,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"50 events - 5 consumers - 1 subscriber": {
			eventCount:         50,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"100 events - 5 consumers - 1 subscriber": {
			eventCount:         100,
			consumerCountLimit: 5,
			subscriberCount:    1,
		},
		"1 event - 5 consumers - 5 subscribers": {
			eventCount:         1,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"10 events - 5 consumers - 5 subscribers": {
			eventCount:         10,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"50 events - 5 consumers - 5 subscribers": {
			eventCount:         50,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"100 events - 5 consumers - 5 subscribers": {
			eventCount:         100,
			consumerCountLimit: 5,
			subscriberCount:    5,
		},
		"1 event - 1 consumer - 5 subscribers": {
			eventCount:         1,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
		"10 events - 1 consumer - 5 subscribers": {
			eventCount:         10,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
		"50 events - 1 consumer - 5 subscribers": {
			eventCount:         50,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
		"100 events - 1 consumer - 5 subscribers": {
			eventCount:         100,
			consumerCountLimit: 1,
			subscriberCount:    5,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testNackAndTimeout(
				t, testCase, eventHandleTypes[timeoutHET],
				WithTimeout(time.Millisecond*1),
				WithRetryCountLimit(3),
			)
		})
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testNackAndTimeout(
				t, testCase, eventHandleTypes[nackHET],
				WithTimeout(time.Second*1),
				WithRetryCountLimit(3),
			)
		})
	}
}

func testNackAndTimeout(
	t *testing.T,
	testCase consumerTestCase,
	handleEvent eventHandle,
	opts ...options.Option,
) {
	Test_DeleteTestFileQueue(t)

	ctx, cancel := context.WithCancel(context.Background())

	ltngqueue, err := New(ctx, opts...)
	require.NoError(t, err)

	queue := &queuemodels.Queue{
		Name: "test-queue",
		Path: "test/queue",

		// consumerCountLimit has no effect if there are no subscribers
		ConsumerCountLimit: testCase.consumerCountLimit,
	}
	_, err = ltngqueue.CreateQueue(ctx, queue)
	require.NoError(t, err)

	op := syncx.NewThreadOperator("publisher", syncx.WithThreadLimit(64))

	// generate & publish events
	events := generateEventList(t, ltngqueue.serializer, queue, testCase.eventCount)
	eventMap := eventListToSafeEventMap(events)
	for i := 0; i < testCase.eventCount; i++ {
		op.Op(func() {
			event := events[i]
			e, err := ltngqueue.Publish(ctx, &queuemodels.Event{
				EventID:  event.EventID,
				Queue:    event.Queue,
				Data:     event.Data,
				Metadata: event.Metadata,
			})
			require.NoError(t, err)
			require.EqualValues(t, event, e)
		})
	}

	op.Wait()

	nodeIdList := make([]string, testCase.subscriberCount)
	receiverList := make([]chan *queuemodels.Event, testCase.subscriberCount)
	for i := 0; i < testCase.subscriberCount; i++ {
		nodeUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

		receiver := make(chan *queuemodels.Event, 1)
		publisher := &queuemodels.Publisher{
			NodeID: nodeID,
			Sender: receiver,
		}
		err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
		require.NoError(t, err)

		nodeIdList[i] = nodeID
		receiverList[i] = receiver
	}

	count := new(atomic.Uint64)
	go func() {
		for _, receiver := range receiverList {
			go func() {
				for event := range receiver {
					handledEvent, err := handleEvent(ltngqueue, event)
					assert.NoError(t, err)
					assert.Equal(t, event.EventID, handledEvent.EventID)
					assert.Equal(t, event.Queue, handledEvent.Queue)
					assert.Equal(t, event.Data, handledEvent.Data)

					e, ok := eventMap.Get(event.EventID)
					assert.True(t, ok)
					assert.EqualValues(t, e.EventID, event.EventID)
					assert.EqualValues(t, e.Queue, event.Queue)
					assert.EqualValues(t, e.Data, event.Data)

					count.Add(1)
				}
			}()
		}
	}()

	for count.Load() != uint64(testCase.eventCount)*4 {
		runtime.Gosched()
	}
	t.Log(count.Load())
	//t.Log("events")
	//for _, event := range events {
	//	t.Log(event)
	//}
	//t.Log("consumedEvents")
	//for _, event := range consumedEvents.Get() {
	//	t.Log(event)
	//}

	for _, nodeID := range nodeIdList {
		err = ltngqueue.UnsubscribeFromQueue(ctx, queue,
			&queuemodels.Publisher{
				NodeID: nodeID,
			})
		require.NoError(t, err)
	}

	time.Sleep(time.Millisecond * 5)

	cancel()
	err = ltngqueue.Close()
	require.NoError(t, err)
}

func generateGenericTestData(t *testing.T) *GenericTestData {
	gtd := &GenericTestData{
		FieldString: random.String(12),
		FieldInt:    int(random.Int(1, 100)),
		FieldBool:   true,
	}

	return gtd
}

func generateGenericTestDataList(t *testing.T, amount int) []*GenericTestData {
	gtdList := make([]*GenericTestData, amount)

	for i := 0; i < amount; i++ {
		gtdList[i] = generateGenericTestData(t)
	}

	return gtdList
}

func generateEventList(
	t *testing.T,
	serializer serializermodels.Serializer,
	queue *queuemodels.Queue,
	amount int,
) []*queuemodels.Event {
	eventList := make([]*queuemodels.Event, amount)

	for i := 0; i < amount; i++ {
		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		gtd := generateGenericTestData(t)
		bs, err := serializer.Serialize(gtd)
		require.NoError(t, err)

		metadata := &queuemodels.EventMetadata{
			Metadata:       nil,
			RetryCount:     0,
			SentAt:         0,
			ReceivedAt:     0,
			ReceivedAtList: nil,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     bs,
			Metadata: metadata,
		}
		eventList[i] = event
	}

	return eventList
}

func eventListToEventMap(
	eventList []*queuemodels.Event,
) map[string]*queuemodels.Event {
	eventMap := make(map[string]*queuemodels.Event)
	for _, event := range eventList {
		eventMap[event.EventID] = event
	}

	return eventMap
}

func eventListToSafeEventMap(
	eventList []*queuemodels.Event,
) *syncx.GenericMap[*queuemodels.Event] {
	eventMap := syncx.NewGenericMap[*queuemodels.Event]()
	for _, event := range eventList {
		eventMap.Set(event.EventID, event)
	}

	return eventMap
}

func TestQueue_Ack(t *testing.T) {
	t.Run("ack", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: &queuemodels.EventMetadata{},
		}
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

		ltngqueue.eventMapTracker.Set(event.EventID, et)

		ackedEvent, err := ltngqueue.Ack(ctx, event)
		assert.NoError(t, err)
		assert.Equal(t, event, ackedEvent)
		assert.True(t, et.WasACKed.Load())
		assert.False(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-ack
		assert.True(t, ok)
		assert.Empty(t, es)
	})

	t.Run("already ack'ed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: &queuemodels.EventMetadata{},
		}
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

		ltngqueue.eventMapTracker.Set(event.EventID, et)
		et.WasACKed.Store(true)

		ackedEvent, err := ltngqueue.Ack(ctx, event)
		assert.NoError(t, err)
		assert.Equal(t, event, ackedEvent)
		assert.True(t, et.WasACKed.Load())
		assert.False(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-ack
		assert.False(t, ok)
		assert.Empty(t, es)
	})

	t.Run("event not found in event tracker", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: &queuemodels.EventMetadata{},
		}
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

		ackedEvent, err := ltngqueue.Ack(ctx, event)
		assert.Error(t, err)
		assert.Nil(t, ackedEvent)
		assert.False(t, et.WasACKed.Load())
		assert.False(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-ack
		assert.False(t, ok)
		assert.Empty(t, es)
	})

	t.Run("invalid event", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: nil,
		}
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

		ackedEvent, err := ltngqueue.Ack(ctx, event)
		assert.Error(t, err)
		assert.Nil(t, ackedEvent)
		assert.False(t, et.WasACKed.Load())
		assert.False(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-ack
		assert.False(t, ok)
		assert.Empty(t, es)
	})
}

func TestQueue_Nack(t *testing.T) {
	t.Run("nack", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: &queuemodels.EventMetadata{},
		}
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

		ltngqueue.eventMapTracker.Set(event.EventID, et)

		nackedEvent, err := ltngqueue.Nack(ctx, event)
		assert.NoError(t, err)
		assert.Equal(t, event, nackedEvent)
		assert.False(t, et.WasACKed.Load())
		assert.True(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-nack
		assert.True(t, ok)
		assert.Empty(t, es)
	})

	t.Run("already nack'ed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: &queuemodels.EventMetadata{},
		}
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

		ltngqueue.eventMapTracker.Set(event.EventID, et)
		et.WasNACKed.Store(true)

		nackedEvent, err := ltngqueue.Nack(ctx, event)
		assert.NoError(t, err)
		assert.Equal(t, event, nackedEvent)
		assert.False(t, et.WasACKed.Load())
		assert.True(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-nack
		assert.False(t, ok)
		assert.Empty(t, es)
	})

	t.Run("event not found in event tracker", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: &queuemodels.EventMetadata{},
		}
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

		nackedEvent, err := ltngqueue.Nack(ctx, event)
		assert.Error(t, err)
		assert.Nil(t, nackedEvent)
		assert.False(t, et.WasACKed.Load())
		assert.False(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-nack
		assert.False(t, ok)
		assert.Empty(t, es)
	})

	t.Run("invalid event", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*5),
		)
		require.NoError(t, err)

		newEventID, err := uuid.NewRandom()
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		event := &queuemodels.Event{
			EventID:  newEventID.String(),
			Queue:    queue,
			Data:     nil,
			Metadata: nil,
		}
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

		nackedEvent, err := ltngqueue.Nack(ctx, event)
		assert.Error(t, err)
		assert.Nil(t, nackedEvent)
		assert.False(t, et.WasACKed.Load())
		assert.False(t, et.WasNACKed.Load())

		close(ack)
		close(nack)
		es, ok := <-nack
		assert.False(t, ok)
		assert.Empty(t, es)
	})
}

func Test_DeleteTestFileQueue(t *testing.T) {
	ctx := context.Background()
	err := osx.DelHard(ctx, ltngDBBasePath)
	require.NoError(t, err)
	err = osx.DelHard(ctx, ltngFileQueueBasePath)
	require.NoError(t, err)
}
