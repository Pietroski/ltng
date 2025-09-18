package ltngqueue_engine

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	serializermodels "gitlab.com/pietroski-software-company/devex/golang/serializer/models"

	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
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

	t.Run("publish only flow", func(t *testing.T) {
		testCases := map[string]struct {
			consumerCountLimit uint32
			eventCount         int
		}{
			"single consumer": {
				consumerCountLimit: 1,
				eventCount:         10,
			},
			"single consumer for one event": {
				consumerCountLimit: 1,
				eventCount:         1,
			},
			"multiple consumers for single event": {
				consumerCountLimit: 5,
				eventCount:         1,
			},
			"multiple consumers for multiple events": {
				consumerCountLimit: 5,
				eventCount:         10,
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

					ConsumerCountLimit: testCase.consumerCountLimit,
				}
				_, err = ltngqueue.CreateQueue(ctx, queue)
				require.NoError(t, err)

				eventCount := testCase.eventCount
				for i := 0; i < eventCount; i++ {
					gtd := &GenericTestData{
						FieldString: go_random.RandomString(12),
						FieldInt:    int(go_random.RandomInt(1, 100)),
						FieldBool:   true,
					}
					bs, err := ltngqueue.serializer.Serialize(gtd)
					require.NoError(t, err)

					metadata := &queuemodels.EventMetadata{
						Metadata:       nil,
						RetryCount:     0,
						SentAt:         0,
						ReceivedAt:     0,
						ReceivedAtList: nil,
					}
					event := &queuemodels.Event{
						EventID:  "",
						Queue:    queue,
						Data:     bs,
						Metadata: metadata,
					}
					e, err := ltngqueue.Publish(ctx, event)
					require.NoError(t, err)
					require.EqualValues(t, event, e)
				}

				time.Sleep(time.Second)

				cancel()
				err = ltngqueue.Close()
				require.NoError(t, err)
			})
		}
	})

	t.Run("subscription / unsubscription only flow", func(t *testing.T) {
		testCases := map[string]struct {
			consumerCountLimit uint32
			subscriberCount    int
		}{
			"single consumer": {
				consumerCountLimit: 1,
				subscriberCount:    10,
			},
			"single consumer for one event": {
				consumerCountLimit: 1,
				subscriberCount:    1,
			},
			"multiple consumers for single event": {
				consumerCountLimit: 5,
				subscriberCount:    1,
			},
			"multiple consumers for multiple events": {
				consumerCountLimit: 5,
				subscriberCount:    10,
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

					ConsumerCountLimit: testCase.consumerCountLimit,
				}
				_, err = ltngqueue.CreateQueue(ctx, queue)
				require.NoError(t, err)

				nodeIdListSize := testCase.subscriberCount
				nodeIdList := make([]string, nodeIdListSize)
				for i := 0; i < testCase.subscriberCount; i++ {
					nodeUUID, err := uuid.NewRandom()
					require.NoError(t, err)
					nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

					nodeIdList[i] = nodeID

					receiver := make(chan *queuemodels.Event, 1)
					publisher := &queuemodels.Publisher{
						NodeID: nodeID,
						Sender: receiver,
					}
					err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
					require.NoError(t, err)
				}

				time.Sleep(1 * time.Second)

				for _, nodeID := range nodeIdList {
					err = ltngqueue.UnsubscribeFromQueue(ctx, queue,
						&queuemodels.Publisher{
							NodeID: nodeID,
						})
					require.NoError(t, err)
				}

				cancel()
				err = ltngqueue.Close()
				require.NoError(t, err)
			})
		}
	})

	t.Run("publish / consumption flow with ack", func(t *testing.T) {
		//t.Skip("wip")

		testCases := map[string]struct {
			consumerCountLimit     uint32
			eventCount             int
			subscriberCount        int
			receiverConcurrentSize int
		}{
			//"single consumer": {
			//	consumerCountLimit:     1,
			//	eventCount:             10,
			//	subscriberCount:        1,
			//	receiverConcurrentSize: 1,
			//},
			//"single consumer for single event": {
			//	consumerCountLimit:     1,
			//	eventCount:             1,
			//	subscriberCount:        1,
			//	receiverConcurrentSize: 1,
			//},
			//"single consumer for one event and multiple receivers": {
			//	consumerCountLimit:     1,
			//	eventCount:             1,
			//	subscriberCount:        1,
			//	receiverConcurrentSize: 10,
			//},
			"single consumer for one event and multiple receivers and subscriber": {
				consumerCountLimit:     1,
				eventCount:             1,
				subscriberCount:        10,
				receiverConcurrentSize: 10,
			},
			//"multiple consumers for single event": {
			//	consumerCountLimit:     5,
			//	eventCount:             1,
			//	subscriberCount:        1,
			//	receiverConcurrentSize: 10,
			//},
			//"multiple consumers for multiple events": {
			//	consumerCountLimit:     5,
			//	eventCount:             10,
			//	subscriberCount:        10,
			//	receiverConcurrentSize: 10,
			//},
		}
		for testName, testCase := range testCases {
			t.Run(testName, func(t *testing.T) {
				Test_DeleteTestFileQueue(t)

				ctx, cancel := context.WithCancel(context.Background())

				ltngqueue, err := New(ctx,
					WithTimeout(time.Second*5),
				)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name: "test-queue",
					Path: "test/queue",

					ConsumerCountLimit: testCase.consumerCountLimit,
				}
				_, err = ltngqueue.CreateQueue(ctx, queue)
				require.NoError(t, err)

				eventCount := testCase.eventCount
				for i := 0; i < eventCount; i++ {
					gtd := &GenericTestData{
						FieldString: go_random.RandomString(12),
						FieldInt:    int(go_random.RandomInt(1, 100)),
						FieldBool:   true,
					}
					bs, err := ltngqueue.serializer.Serialize(gtd)
					require.NoError(t, err)

					metadata := &queuemodels.EventMetadata{
						Metadata:       nil,
						RetryCount:     0,
						SentAt:         0,
						ReceivedAt:     0,
						ReceivedAtList: nil,
					}
					event := &queuemodels.Event{
						EventID:  "",
						Queue:    queue,
						Data:     bs,
						Metadata: metadata,
					}

					e, err := ltngqueue.Publish(ctx, event)
					require.NoError(t, err)
					require.EqualValues(t, event, e)
				}

				type subscriberTracker struct {
					nodeID   string
					receiver chan *queuemodels.Event
				}

				subscriberListSize := testCase.subscriberCount
				subscriberList := make([]*subscriberTracker, subscriberListSize)
				for i := 0; i < testCase.subscriberCount; i++ {
					nodeUUID, err := uuid.NewRandom()
					require.NoError(t, err)
					nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

					receiver := make(chan *queuemodels.Event, testCase.receiverConcurrentSize)
					publisher := &queuemodels.Publisher{
						NodeID: nodeID,
						Sender: receiver,
					}
					err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
					require.NoError(t, err)

					subscriberList[i] = &subscriberTracker{
						nodeID:   nodeID,
						receiver: receiver,
					}
				}

				count := new(atomic.Uint64)
				go func() {
					for _, subscriber := range subscriberList {
						go func() {
							for event := range subscriber.receiver {
								count.Add(1)
								fmt.Printf("received event: %+v - on node id: %+v\n",
									event.EventID, subscriber.nodeID)
								_, err := ltngqueue.Ack(ctx, event)
								assert.NoError(t, err)
							}
						}()
					}
				}()

				for count.Load() != uint64(eventCount) {
					runtime.Gosched()
				}
				assert.EqualValues(t, uint64(eventCount), count.Load())

				time.Sleep(time.Second)

				cancel()
				err = ltngqueue.Close()
				require.NoError(t, err)
			})
		}
	})

	t.Run("publish / consumption with ack/nack timeout flow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx, WithTimeout(time.Second))
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 5,
		}
		_, err = ltngqueue.CreateQueue(ctx, queue)
		require.NoError(t, err)

		eventCount := 10
		for i := 0; i < eventCount; i++ {
			gtd := &GenericTestData{
				FieldString: go_random.RandomString(12),
				FieldInt:    int(go_random.RandomInt(1, 100)),
				FieldBool:   true,
			}
			bs, err := ltngqueue.serializer.Serialize(gtd)
			require.NoError(t, err)

			metadata := &queuemodels.EventMetadata{
				Metadata:       nil,
				RetryCount:     0,
				SentAt:         0,
				ReceivedAt:     0,
				ReceivedAtList: nil,
			}
			event := &queuemodels.Event{
				EventID:  "",
				Queue:    queue,
				Data:     bs,
				Metadata: metadata,
			}

			e, err := ltngqueue.Publish(ctx, event)
			require.NoError(t, err)
			require.EqualValues(t, event, e)
		}

		nodeUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

		receiver := make(chan *queuemodels.Event, 10)
		publisher := &queuemodels.Publisher{
			NodeID: nodeID,
			Sender: receiver,
		}
		err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
		require.NoError(t, err)

		count := new(atomic.Uint64)
		go func() {
			for event := range receiver {
				count.Add(1)
				fmt.Printf("received event: %+v\n", event)
			}
		}()

		time.Sleep(time.Second * 2)
		assert.EqualValues(t, uint64(eventCount), count.Load())
		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("publish / consumption with ack timeout flow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx, WithTimeout(time.Second))
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 5,
		}
		_, err = ltngqueue.CreateQueue(ctx, queue)
		require.NoError(t, err)

		eventCount := 10
		for i := 0; i < eventCount; i++ {
			gtd := &GenericTestData{
				FieldString: go_random.RandomString(12),
				FieldInt:    int(go_random.RandomInt(1, 100)),
				FieldBool:   true,
			}
			bs, err := ltngqueue.serializer.Serialize(gtd)
			require.NoError(t, err)

			metadata := &queuemodels.EventMetadata{
				Metadata:       nil,
				RetryCount:     0,
				SentAt:         0,
				ReceivedAt:     0,
				ReceivedAtList: nil,
			}
			event := &queuemodels.Event{
				EventID:  "",
				Queue:    queue,
				Data:     bs,
				Metadata: metadata,
			}

			e, err := ltngqueue.Publish(ctx, event)
			require.NoError(t, err)
			require.EqualValues(t, event, e)
		}

		nodeUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

		receiver := make(chan *queuemodels.Event, 10)
		publisher := &queuemodels.Publisher{
			NodeID: nodeID,
			Sender: receiver,
		}
		err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
		require.NoError(t, err)

		count := new(atomic.Uint64)
		go func() {
			for event := range receiver {
				count.Add(1)
				fmt.Printf("received event: %+v\n", event)
				_, err := ltngqueue.Ack(ctx, event)
				assert.NoError(t, err)
			}
		}()

		time.Sleep(time.Second * 2)
		assert.EqualValues(t, uint64(eventCount), count.Load())
		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("publish / consumption with nack timeout flow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		retryCountLimit := uint64(3)
		ltngqueue, err := New(ctx,
			WithTimeout(time.Second*30),
			WithRetryCountLimit(retryCountLimit),
		)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 1,
		}
		_, err = ltngqueue.CreateQueue(ctx, queue)
		require.NoError(t, err)

		eventCount := 1
		for i := 0; i < eventCount; i++ {
			gtd := &GenericTestData{
				FieldString: go_random.RandomString(12),
				FieldInt:    int(go_random.RandomInt(1, 100)),
				FieldBool:   true,
			}
			bs, err := ltngqueue.serializer.Serialize(gtd)
			require.NoError(t, err)

			metadata := &queuemodels.EventMetadata{
				Metadata:       nil,
				RetryCount:     0,
				SentAt:         0,
				ReceivedAt:     0,
				ReceivedAtList: nil,
			}
			event := &queuemodels.Event{
				EventID:  "",
				Queue:    queue,
				Data:     bs,
				Metadata: metadata,
			}

			e, err := ltngqueue.Publish(ctx, event)
			require.NoError(t, err)
			require.EqualValues(t, event, e)
		}

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
		go func() {
			for event := range receiver {
				count.Add(1)
				fmt.Printf("received event: %+v\n", event)
				_, err := ltngqueue.Nack(ctx, event)
				assert.NoError(t, err)
			}
		}()

		for count.Load() != 3 {
			runtime.Gosched()
		}

		t.Log("canceling")
		time.Sleep(time.Second * 1)
		t.Log(count.Load())
		assert.EqualValues(t, uint64(eventCount)+retryCountLimit, count.Load())
		cancel()
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

		time.Sleep(time.Second)
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
		"500 events": {
			eventCount: 500,
		},
		"1_000 events": {
			eventCount: 1_000,
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
				err = qs.FileQueue.ReaderPooler(ctx, func(ctx context.Context, bs []byte) error {
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
				assert.NoError(t, err)
			}()

			for count.Load() != uint64(testCase.eventCount) {
				runtime.Gosched()
			}
			t.Log(count.Load())

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)
		})
	}
}

func generateGenericTestData(t *testing.T) *GenericTestData {
	gtd := &GenericTestData{
		FieldString: go_random.RandomString(12),
		FieldInt:    int(go_random.RandomInt(1, 100)),
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
			EventID:  "",
			Queue:    queue,
			Data:     bs,
			Metadata: metadata,
		}
		eventList[i] = event
	}

	return eventList
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

func TestQueue_AckFlow(t *testing.T) {
	testCases := map[string]struct {
		consumerCountLimit     uint32
		eventCount             int
		subscriberCount        int
		receiverConcurrentSize int
	}{
		"single consumer": {
			consumerCountLimit:     1,
			eventCount:             10,
			subscriberCount:        1,
			receiverConcurrentSize: 1,
		},
		"single consumer for many events": {
			consumerCountLimit:     1,
			eventCount:             50,
			subscriberCount:        1,
			receiverConcurrentSize: 1,
		},
		"single consumer for single event": {
			consumerCountLimit:     1,
			eventCount:             1,
			subscriberCount:        1,
			receiverConcurrentSize: 1,
		},
		"single consumer for one event and multiple receivers": {
			consumerCountLimit:     1,
			eventCount:             1,
			subscriberCount:        1,
			receiverConcurrentSize: 10,
		},
		"single consumer for one event and multiple receivers and subscribers": {
			consumerCountLimit:     1,
			eventCount:             1,
			subscriberCount:        10,
			receiverConcurrentSize: 10,
		},
		"single consumer for multiple events and multiple receivers and subscribers": {
			consumerCountLimit:     1,
			eventCount:             10,
			subscriberCount:        10,
			receiverConcurrentSize: 10,
		},
		"single consumer for many multiple events and multiple receivers and subscribers": {
			consumerCountLimit:     1,
			eventCount:             100,
			subscriberCount:        10,
			receiverConcurrentSize: 10,
		},
		"multiple consumers, receivers and subscribers for many multiple events": {
			consumerCountLimit:     10,
			eventCount:             100,
			subscriberCount:        10,
			receiverConcurrentSize: 10,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			Test_DeleteTestFileQueue(t)

			ctx, cancel := context.WithCancel(context.Background())

			ltngqueue, err := New(ctx,
				WithTimeout(time.Second*5),
			)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name: "test-queue",
				Path: "test/queue",

				ConsumerCountLimit: testCase.consumerCountLimit,
			}
			_, err = ltngqueue.CreateQueue(ctx, queue)
			require.NoError(t, err)

			eventCount := testCase.eventCount
			for i := 0; i < eventCount; i++ {
				gtd := &GenericTestData{
					FieldString: go_random.RandomString(12),
					FieldInt:    int(go_random.RandomInt(1, 100)),
					FieldBool:   true,
				}
				bs, err := ltngqueue.serializer.Serialize(gtd)
				require.NoError(t, err)

				metadata := &queuemodels.EventMetadata{
					Metadata:       nil,
					RetryCount:     0,
					SentAt:         0,
					ReceivedAt:     0,
					ReceivedAtList: nil,
				}
				event := &queuemodels.Event{
					EventID:  "",
					Queue:    queue,
					Data:     bs,
					Metadata: metadata,
				}

				e, err := ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				require.EqualValues(t, event, e)
			}

			type subscriberTracker struct {
				nodeID   string
				receiver chan *queuemodels.Event
			}

			subscriberListSize := testCase.subscriberCount
			subscriberList := make([]*subscriberTracker, subscriberListSize)
			for i := 0; i < testCase.subscriberCount; i++ {
				nodeUUID, err := uuid.NewRandom()
				require.NoError(t, err)
				nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

				receiver := make(chan *queuemodels.Event, testCase.receiverConcurrentSize)
				publisher := &queuemodels.Publisher{
					NodeID: nodeID,
					Sender: receiver,
				}
				err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
				require.NoError(t, err)

				subscriberList[i] = &subscriberTracker{
					nodeID:   nodeID,
					receiver: receiver,
				}
			}

			count := new(atomic.Uint64)
			go func() {
				for _, subscriber := range subscriberList {
					go func() {
						for event := range subscriber.receiver {
							count.Add(1)
							fmt.Printf("received event: %+v - on node id: %+v\n",
								event.EventID, subscriber.nodeID)
							_, err := ltngqueue.Ack(ctx, event)
							assert.NoError(t, err)
						}
					}()
				}
			}()

			for count.Load() != uint64(eventCount) {
				runtime.Gosched()
			}
			assert.EqualValues(t, uint64(eventCount), count.Load())

			time.Sleep(time.Second)

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)
		})
	}
}

func Test_DeleteTestFileQueue(t *testing.T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngDBBasePath)
	require.NoError(t, err)
	_, err = execx.DelHardExec(ctx, ltngFileQueueBasePath)
	require.NoError(t, err)
}
