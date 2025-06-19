package ltngqueue_engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"
	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

func TestQueueFlow(t *testing.T) {
	t.Skip("Refactor")
	t.Run("single thread", func(t *testing.T) {
		t.Run("successfully publish but do not consume - test close engines", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ltngqueue, err := New(ctx)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name:                  "test_round_robin",
				Path:                  "test/round_robin",
				QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
			}
			_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
			require.NoError(t, err)

			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "str",
					IntField:  1,
					BoolField: true,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
			}

			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "any-other-str",
					IntField:  9,
					BoolField: false,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
			}

			ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
				// close(value.SignalTransmitter)
				value.IsClosed.Store(true)
				return true
			})

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)

			qs, err := ltngqueue.getQueueSignaler(ctx, queue)
			require.NoError(t, err)
			assertFQCount(t, ctx, qs.FileQueue, 2)
		})

		t.Run("successfully ack an event", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ltngqueue, err := New(ctx)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name:                  "test_round_robin",
				Path:                  "test/round_robin",
				QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
			}
			_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
			require.NoError(t, err)

			var eventID1, eventID2 string
			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "str",
					IntField:  1,
					BoolField: true,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				eventID1 = event.EventID
			}
			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "any-other-str",
					IntField:  9,
					BoolField: false,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				eventID2 = event.EventID
			}
			t.Log(eventID1, eventID2)

			{
				nodeUUID, errNewRand := uuid.NewRandom()
				require.NoError(t, errNewRand)
				nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

				receiver := make(chan *queuemodels.Event, 1)
				publisher := &queuemodels.Publisher{
					NodeID: nodeID,
					Sender: receiver,
				}
				subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
				require.NoError(t, subErr)
				event := <-receiver
				assert.Equal(t, eventID1, event.EventID)
				event, ackErr := ltngqueue.Ack(ctx, event)
				require.NoError(t, ackErr)
				t.Log(event)

				event = <-receiver
				assert.Equal(t, eventID2, event.EventID)
				event, ackErr = ltngqueue.Ack(ctx, event)
				require.NoError(t, ackErr)
				t.Log(event)
			}

			ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
				// close(value.SignalTransmitter)
				value.IsClosed.Store(true)
				return true
			})

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)

			qs, err := ltngqueue.getQueueSignaler(ctx, queue)
			require.NoError(t, err)
			assertFQCount(t, ctx, qs.FileQueue, 0)
		})

		t.Run("successfully nack an event", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ltngqueue, err := New(ctx)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name:                  "test_round_robin",
				Path:                  "test/round_robin",
				QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
			}
			_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
			require.NoError(t, err)

			var eventID1, eventID2 string
			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "str",
					IntField:  1,
					BoolField: true,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				eventID1 = event.EventID
			}
			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "any-other-str",
					IntField:  9,
					BoolField: false,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				eventID2 = event.EventID
			}
			t.Log(eventID1, eventID2)

			{
				nodeUUID, errNewRand := uuid.NewRandom()
				require.NoError(t, errNewRand)
				nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

				receiver := make(chan *queuemodels.Event, 1)
				publisher := &queuemodels.Publisher{
					NodeID: nodeID,
					Sender: receiver,
				}
				subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
				require.NoError(t, subErr)
				event := <-receiver
				assert.Equal(t, eventID1, event.EventID)
				event, ackErr := ltngqueue.Nack(ctx, event)
				require.NoError(t, ackErr)
				t.Log(event)

				event = <-receiver
				assert.Equal(t, eventID2, event.EventID)
				event, ackErr = ltngqueue.Nack(ctx, event)
				require.NoError(t, ackErr)
				t.Log(event)
			}

			ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
				// close(value.SignalTransmitter)
				value.IsClosed.Store(true)
				return true
			})

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)

			qs, err := ltngqueue.getQueueSignaler(ctx, queue)
			require.NoError(t, err)
			assertFQCount(t, ctx, qs.FileQueue, 2)
		})

		t.Run("failed - event timout error", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
			require.NoError(t, err)
			require.NoError(t, err)

			queue := &queuemodels.Queue{
				Name:                  "test_round_robin",
				Path:                  "test/round_robin",
				QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
			}
			_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
			require.NoError(t, err)

			var eventID1, eventID2 string
			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "str",
					IntField:  1,
					BoolField: true,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				eventID1 = event.EventID
			}
			{
				s := serializer.NewRawBinarySerializer()
				eventTestData := &EventTestData{
					StrField:  "any-other-str",
					IntField:  9,
					BoolField: false,
				}
				bs, err := s.Serialize(eventTestData)
				require.NoError(t, err)

				event := &queuemodels.Event{
					Queue: queue,
					Data:  bs,
					Metadata: &queuemodels.EventMetadata{
						SentAt: time.Now().UTC().Unix(),
					},
				}
				event, err = ltngqueue.Publish(ctx, event)
				require.NoError(t, err)
				eventID2 = event.EventID
			}
			t.Log(eventID1, eventID2)

			{
				nodeUUID, errNewRand := uuid.NewRandom()
				require.NoError(t, errNewRand)
				nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()

				receiver := make(chan *queuemodels.Event, 1)
				publisher := &queuemodels.Publisher{
					NodeID: nodeID,
					Sender: receiver,
				}
				subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
				require.NoError(t, subErr)
				event := <-receiver
				assert.Equal(t, eventID1, event.EventID)
				t.Log(event)

				time.Sleep(time.Second * 2)
			}

			ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
				// close(value.SignalTransmitter)
				value.IsClosed.Store(true)
				return true
			})

			cancel()
			err = ltngqueue.Close()
			require.NoError(t, err)

			qs, err := ltngqueue.getQueueSignaler(ctx, queue)
			require.NoError(t, err)
			assertFQCount(t, ctx, qs.FileQueue, 2)
		})
	})

	t.Run("multi-thread", func(t *testing.T) {
		// t.Cleanup(func() { TestReadFromFQGroups(t) })

		t.Run("successfully test round robin propagation", func(t *testing.T) {
			t.Run("for two", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				ltngqueue, err := New(ctx)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				var eventID1, eventID2 string
				{
					s := serializer.NewRawBinarySerializer()
					eventTestData := &EventTestData{
						StrField:  "str",
						IntField:  1,
						BoolField: true,
					}
					bs, err := s.Serialize(eventTestData)
					require.NoError(t, err)

					event := &queuemodels.Event{
						Queue: queue,
						Data:  bs,
						Metadata: &queuemodels.EventMetadata{
							SentAt: time.Now().UTC().Unix(),
						},
					}
					event, err = ltngqueue.Publish(ctx, event)
					require.NoError(t, err)
					eventID1 = event.EventID
				}
				{
					s := serializer.NewRawBinarySerializer()
					eventTestData := &EventTestData{
						StrField:  "any-other-str",
						IntField:  9,
						BoolField: false,
					}
					bs, err := s.Serialize(eventTestData)
					require.NoError(t, err)

					event := &queuemodels.Event{
						Queue: queue,
						Data:  bs,
						Metadata: &queuemodels.EventMetadata{
							SentAt: time.Now().UTC().Unix(),
						},
					}
					event, err = ltngqueue.Publish(ctx, event)
					require.NoError(t, err)
					eventID2 = event.EventID
				}
				t.Log(eventID1, eventID2)

				op := concurrent.New("test_round_robin")
				op.Op(func() {
					nodeUUID, errNewRand := uuid.NewRandom()
					require.NoError(t, errNewRand)
					nodeID := queue.GetCompleteLockKey() + "/" + nodeUUID.String()

					receiver := make(chan *queuemodels.Event, 1)
					publisher := &queuemodels.Publisher{
						NodeID: nodeID,
						Sender: receiver,
					}
					subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
					require.NoError(t, subErr)
					event := <-receiver
					assert.Equal(t, eventID1, event.EventID)
					time.Sleep(150 * time.Millisecond)
					event, ackErr := ltngqueue.Ack(ctx, event)
					require.NoError(t, ackErr)
					t.Logf("%+v\n", publisher)
					t.Log(event)
				})
				time.Sleep(150 * time.Millisecond)
				op.Op(func() {
					nodeUUID, errNewRand := uuid.NewRandom()
					require.NoError(t, errNewRand)
					nodeID := queue.GetCompleteLockKey() + "/" + nodeUUID.String()

					receiver := make(chan *queuemodels.Event, 1)
					publisher := &queuemodels.Publisher{
						NodeID: nodeID,
						Sender: receiver,
					}
					subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
					require.NoError(t, subErr)
					event := <-receiver
					assert.Equal(t, eventID2, event.EventID)
					event, ackErr := ltngqueue.Ack(ctx, event)
					require.NoError(t, ackErr)
					t.Logf("%+v\n", publisher)
					t.Log(event)
				})
				err = op.WaitAndWrapErr()
				require.NoError(t, err)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				cancel()
				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for two - generic", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				ltngqueue, err := New(ctx)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				consumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				cancel()
				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for two events, two continuous generic consumers - with no order", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, two continuous generic consumers - with no order", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 20, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, many continuous generic consumers - with no order", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 40, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousConsumerReplicaSet(t, ctx, ltngqueue, 4, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})
		})

		t.Run("successfully test fan out propagation", func(t *testing.T) {
			t.Run("for two", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_fanout",
					Path:                  "test/fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, two generic continuous consumer replicas", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_fanout",
					Path:                  "test/fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 20, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, many generic continuous consumer replicas", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_fanout",
					Path:                  "test/fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT,
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 40, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 4, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})
		})

		t.Run("successfully test round robin group propagation", func(t *testing.T) {
			t.Run("for two", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				ltngqueue, err := New(ctx)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				consumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				cancel()
				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for two events, two continuous generic consumers - with no order", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, two continuous generic consumers - with no order", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 20, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, many continuous generic consumers - with no order", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_round_robin",
					Path:                  "test/round_robin",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 40, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousConsumerReplicaSet(t, ctx, ltngqueue, 4, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})
		})

		t.Run("successfully test fan out group propagation", func(t *testing.T) {
			t.Run("for two", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_fanout",
					Path:                  "test/fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_FAN_OUT,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, two generic continuous consumer replicas", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_fanout",
					Path:                  "test/fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_FAN_OUT,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 20, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 2, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			t.Run("for many events, many generic continuous consumer replicas", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name:                  "test_fanout",
					Path:                  "test/fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_FAN_OUT,
					Group: &queuemodels.Group{
						Name: "test_group",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, queue)
				require.NoError(t, err)

				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 40, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}
				continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 4, queue, eventDataWrapperList)

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})
		})

		t.Run("successfully test round robin and fan out group propagation", func(t *testing.T) {
			t.Run("for two", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				groupRoundRobinQueue := &queuemodels.Queue{
					Name:                  "round_robin|fanout",
					Path:                  "test/round_robin|fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN,
					Group: &queuemodels.Group{
						Name: "test_group|round_robin",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, groupRoundRobinQueue)
				require.NoError(t, err)

				groupFanOutQueue := &queuemodels.Queue{
					Name:                  "round_robin|fanout",
					Path:                  "test/round_robin|fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_FAN_OUT,
					Group: &queuemodels.Group{
						Name: "test_group|fanout",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, groupFanOutQueue)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name: "round_robin|fanout",
					Path: "test/round_robin|fanout",
				}
				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 2, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}

				op := concurrent.New("consumers")
				op.Op(func() {
					roundRobinQueue := &queuemodels.Queue{
						Name:                  "round_robin|fanout",
						Path:                  "test/round_robin|fanout",
						QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
						Group: &queuemodels.Group{
							Name: "test_group|round_robin",
						},
					}
					continuousLiveConsumerReplicaSet(t, ctx, ltngqueue, 2, roundRobinQueue, eventDataWrapperList)
				})
				op.Op(func() {
					fanOutQueue := &queuemodels.Queue{
						Name:                  "round_robin|fanout",
						Path:                  "test/round_robin|fanout",
						QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT,
						Group: &queuemodels.Group{
							Name: "test_group|fanout",
						},
					}
					continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 2, fanOutQueue, eventDataWrapperList)
				})
				op.Wait()

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})

			// TODO: redesign the group subscription flow
			t.Run("for many events, two generic continuous consumer replicas", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
				defer cancel()
				ltngqueue, err := New(ctx, WithTimeout(time.Second*1))
				require.NoError(t, err)
				require.NoError(t, err)

				groupRoundRobinQueue := &queuemodels.Queue{
					Name:                  "round_robin|fanout",
					Path:                  "test/round_robin|fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_ROUND_ROBIN,
					Group: &queuemodels.Group{
						Name: "test_group|round_robin",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, groupRoundRobinQueue)
				require.NoError(t, err)

				groupFanOutQueue := &queuemodels.Queue{
					Name:                  "round_robin|fanout",
					Path:                  "test/round_robin|fanout",
					QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_GROUP_FAN_OUT,
					Group: &queuemodels.Group{
						Name: "test_group|fanout",
					},
				}
				_, err = ltngqueue.CreateQueueSignaler(ctx, groupFanOutQueue)
				require.NoError(t, err)

				queue := &queuemodels.Queue{
					Name: "round_robin|fanout",
					Path: "test/round_robin|fanout",
				}
				eventDataWrapperList := generateAndPublishRandomEvents(t, ltngqueue, 3, queue)
				for _, eventDataWrapper := range eventDataWrapperList {
					t.Logf("%+v\n", eventDataWrapper)
				}

				op := concurrent.New("consumers")
				op.Op(func() {
					roundRobinQueue := &queuemodels.Queue{
						Name:                  "round_robin|fanout",
						Path:                  "test/round_robin|fanout",
						QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN,
						Group: &queuemodels.Group{
							Name: "test_group|round_robin",
						},
					}
					continuousLiveConsumerReplicaSet(t, ctx, ltngqueue, 2,
						roundRobinQueue, eventDataWrapperList)
				})
				op.Op(func() {
					fanOutQueue := &queuemodels.Queue{
						Name:                  "round_robin|fanout",
						Path:                  "test/round_robin|fanout",
						QueueDistributionType: queuemodels.QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT,
						Group: &queuemodels.Group{
							Name: "test_group|fanout",
						},
					}
					continuousFanoutConsumerReplicaSet(t, ctx, ltngqueue, 2, fanOutQueue, eventDataWrapperList)
				})
				op.Wait()

				ltngqueue.fqMapping.Range(func(key string, value *queuemodels.QueueSignaler) bool {
					// close(value.SignalTransmitter)
					value.IsClosed.Store(true)
					return true
				})

				err = ltngqueue.Close()
				require.NoError(t, err)

				qs, err := ltngqueue.getQueueSignaler(ctx, queue)
				require.NoError(t, err)
				assertFQCount(t, ctx, qs.FileQueue, 0)
			})
		})
	})
}

type (
	EventTestDataWrapper struct {
		EventID       string
		WasACKed      *atomic.Bool
		Event         *queuemodels.Event
		EventTestData *EventTestData
	}

	EventTestData struct {
		StrField  string
		IntField  int
		BoolField bool
	}
)

func generateRandomEvent(
	t *testing.T, queue *queuemodels.Queue,
) *EventTestDataWrapper {
	s := serializer.NewRawBinarySerializer()
	eventTestData := &EventTestData{
		StrField:  go_random.RandomStringWithPrefixWithSep(10, "str_field", "-"),
		IntField:  int(go_random.RandomInt(1, 100)),
		BoolField: go_random.RandomInt(0, 1) == 0,
	}
	bs, err := s.Serialize(eventTestData)
	require.NoError(t, err)

	event := &queuemodels.Event{
		EventID: go_random.RandomStringWithPrefixWithSep(10, "event_id", "-"),
		Queue:   queue,
		Data:    bs,
		Metadata: &queuemodels.EventMetadata{
			SentAt: time.Now().UTC().Unix(),
		},
	}

	return &EventTestDataWrapper{
		EventID:       event.EventID,
		WasACKed:      new(atomic.Bool),
		Event:         event,
		EventTestData: eventTestData,
	}
}

func generateRandomEvents(
	t *testing.T, amount int, queue *queuemodels.Queue,
) []*EventTestDataWrapper {
	var events []*EventTestDataWrapper
	for i := 0; i < amount; i++ {
		events = append(events, generateRandomEvent(t, queue))
	}

	return events
}

func generateAndPublishRandomEvents(
	t *testing.T, ltngqueue *Queue, amount int, queue *queuemodels.Queue,
) []*EventTestDataWrapper {
	events := generateRandomEvents(t, amount, queue)

	for _, eventTestData := range events {
		event, err := ltngqueue.Publish(context.Background(), eventTestData.Event)
		require.NoError(t, err)

		eventTestData.EventID = event.EventID
	}

	return events
}

func EventTestDataWrapperToMap(
	eventDataWrapperList []*EventTestDataWrapper,
) *safe.GenericMap[*EventTestDataWrapper] {
	eventDataWrapperMap := safe.NewGenericMap[*EventTestDataWrapper]()

	for _, eventDataWrapper := range eventDataWrapperList {
		eventDataWrapperMap.Set(eventDataWrapper.EventID, eventDataWrapper)
	}

	return eventDataWrapperMap
}

func consumerReplicaSet(
	t *testing.T, ctx context.Context,
	ltngqueue *Queue, amount int,
	queue *queuemodels.Queue,
	eventDataWrapperList []*EventTestDataWrapper,
) {
	op := concurrent.New("test_round_robin")
	for i := 0; i < amount; i++ {
		op.Op(func() {
			nodeUUID, errNewRand := uuid.NewRandom()
			require.NoError(t, errNewRand)
			nodeID := queue.GetCompleteLockKey() + "/" + nodeUUID.String()

			receiver := make(chan *queuemodels.Event, 1)
			publisher := &queuemodels.Publisher{
				NodeID: nodeID,
				Sender: receiver,
			}
			subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
			require.NoError(t, subErr)
			event := <-receiver
			assert.Equal(t, eventDataWrapperList[i].EventID, event.EventID)
			time.Sleep(150 * time.Millisecond)
			event, ackErr := ltngqueue.Ack(ctx, event)
			require.NoError(t, ackErr)
			t.Logf("%+v\n", publisher)
			t.Log(event)
		})
		time.Sleep(150 * time.Millisecond)
	}
	err := op.WaitAndWrapErr()
	require.NoError(t, err)
}

func continuousConsumerReplicaSet(
	t *testing.T, ctx context.Context,
	ltngqueue *Queue, amount int,
	queue *queuemodels.Queue,
	eventDataWrapperList []*EventTestDataWrapper,
) {
	eventDataWrapperMap := EventTestDataWrapperToMap(eventDataWrapperList)

	op := concurrent.New("continuous_consumer_replica_set")
	for i := 0; i < amount; i++ {
		op.Op(func() {
			nodeUUID, errNewRand := uuid.NewRandom()
			require.NoError(t, errNewRand)
			nodeID := queue.GetCompleteLockKey() + "/" + nodeUUID.String()

			receiver := make(chan *queuemodels.Event)
			publisher := &queuemodels.Publisher{
				NodeID: nodeID,
				Sender: receiver,
			}
			subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
			require.NoError(t, subErr)

			for {
				select {
				case event := <-receiver:
					t.Logf("eventID: %v - nodeID: %v\n", event.EventID, nodeID)
					expectedEvent, ok := eventDataWrapperMap.Get(event.EventID)
					assert.True(t, ok)
					wasACKed := expectedEvent.WasACKed.Load()
					assert.False(t, wasACKed)

					event, ackErr := ltngqueue.Ack(ctx, event)
					assert.NoError(t, ackErr)

					expectedEvent.WasACKed.Store(true)
				case <-ctx.Done():
					t.Logf("test context cancelling: %v", nodeID)
					return
				}
			}
		})
	}
	op.Wait()
}

func continuousFanoutConsumerReplicaSet(
	t *testing.T, ctx context.Context,
	ltngqueue *Queue, amount int,
	queue *queuemodels.Queue,
	eventDataWrapperList []*EventTestDataWrapper,
) {
	eventDataWrapperMap := EventTestDataWrapperToMap(eventDataWrapperList)

	op := concurrent.New("continuous_consumer_replica_set")
	for i := 0; i < amount; i++ {
		op.Op(func() {
			nodeUUID, errNewRand := uuid.NewRandom()
			require.NoError(t, errNewRand)
			nodeID := queue.GetCompleteLockKey() + "/" + nodeUUID.String()

			receiver := make(chan *queuemodels.Event)
			publisher := &queuemodels.Publisher{
				NodeID: nodeID,
				Sender: receiver,
			}
			subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
			require.NoError(t, subErr)

			for {
				select {
				case event := <-receiver:
					t.Logf("eventID: %v - nodeID: %v\n", event.EventID, nodeID)
					expectedEvent, ok := eventDataWrapperMap.Get(event.EventID)
					assert.True(t, ok)
					wasACKed := expectedEvent.WasACKed.Load()
					if wasACKed {
						continue
					}

					event, ackErr := ltngqueue.Ack(ctx, event)
					if ackErr == nil {
						expectedEvent.WasACKed.Store(true)
						return
					}
				case <-ctx.Done():
					t.Logf("test context cancelling: %v", nodeID)
					return
				}
			}
		})
	}
	op.Wait()
}

func continuousLiveConsumerReplicaSet(
	t *testing.T, ctx context.Context,
	ltngqueue *Queue, amount int,
	queue *queuemodels.Queue,
	eventDataWrapperList []*EventTestDataWrapper,
) {
	eventDataWrapperMap := EventTestDataWrapperToMap(eventDataWrapperList)

	op := concurrent.New("continuous_consumer_replica_set")
	for i := 0; i < amount; i++ {
		op.Op(func() {
			nodeUUID, errNewRand := uuid.NewRandom()
			require.NoError(t, errNewRand)
			nodeID := queue.GetCompleteLockKey() + "/" + nodeUUID.String()

			receiver := make(chan *queuemodels.Event)
			publisher := &queuemodels.Publisher{
				NodeID: nodeID,
				Sender: receiver,
			}
			subErr := ltngqueue.SubscribeToQueue(ctx, queue, publisher)
			require.NoError(t, subErr)

			for {
				select {
				case event := <-receiver:
					t.Logf("eventID: %v - nodeID: %v\n", event.EventID, nodeID)
					expectedEvent, ok := eventDataWrapperMap.Get(event.EventID)
					assert.True(t, ok)
					wasACKed := expectedEvent.WasACKed.Load()
					if wasACKed {
						continue
					}

					event, ackErr := ltngqueue.Ack(ctx, event)
					if ackErr == nil {
						expectedEvent.WasACKed.Store(true)
					}
				case <-ctx.Done():
					t.Logf("test context cancelling: %v", nodeID)
					return
				}
			}
		})
	}
	op.Wait()
}

func TestReadAll(t *testing.T) {
	t.Skip("Refactor")
	TestReadFromFQ(t)
	TestReadFromFQGroup(t)
	TestReadFromFQGroups(t)
}

func TestReadFromFQ(t *testing.T) {
	t.Skip("Refactor")

	ctx := context.Background()
	{
		t.Log("test_round_robin")
		fq, err := filequeuev1.New(ctx,
			"test/round_robin", "test_round_robin")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}

	{
		t.Log("test_fanout")
		fq, err := filequeuev1.New(ctx,
			"test/fanout", "test_fanout")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}
}

func TestReadFromFQGroup(t *testing.T) {
	t.Skip("Refactor")

	ctx := context.Background()
	{
		t.Log("test_round_robin|test_group")
		fq, err := filequeuev1.New(ctx,
			"test/round_robin", "test_round_robin|test_group")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}

	{
		t.Log("test_fanout|test_group")
		fq, err := filequeuev1.New(ctx,
			"test/fanout", "test_fanout|test_group")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}
}

func TestReadFromFQGroups(t *testing.T) {
	t.Skip("Refactor")

	ctx := context.Background()
	{
		t.Log("round_robin|fanout")
		fq, err := filequeuev1.New(ctx,
			"test/round_robin|fanout", "round_robin|fanout")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}

	{
		t.Log("round_robin|fanout|test_group")
		fq, err := filequeuev1.New(ctx,
			"test/round_robin|fanout", "round_robin|fanout|test_group|round_robin")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}

	{
		t.Log("fanout|test_group")
		fq, err := filequeuev1.New(ctx,
			"test/round_robin|fanout", "round_robin|fanout|test_group|fanout")
		require.NoError(t, err)
		fqCount(t, ctx, fq)
	}
}

func assertFQCount(
	t *testing.T, ctx context.Context,
	fq *filequeuev1.FileQueue, expected int,
) {
	counter := fqCount(t, ctx, fq)
	assert.Equal(t, expected, counter)
}

func fqCount(t *testing.T, ctx context.Context, fq *filequeuev1.FileQueue) int {
	var counter int
	for {
		_, err := fq.Read(ctx)
		if err != nil {
			t.Log(err)
			break
		}

		err = fq.Pop(ctx)
		assert.NoError(t, err)

		counter++
	}
	t.Log(counter)

	return counter
}
