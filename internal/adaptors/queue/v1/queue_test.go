package ltngqueue_engine

import (
	"context"
	"github.com/google/uuid"
	"gitlab.com/pietroski-software-company/devex/golang/concurrent"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
)

type EventTestData struct {
	StrField  string
	IntField  int
	BoolField bool
}

func TestQueueFlow(t *testing.T) {
	t.Run("successfully test round robin propagation", func(t *testing.T) {
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

		_, _ = eventID1, eventID2

		op := concurrent.New("test_round_robin")
		op.OpX(func() (any, error) {
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
			event := <-receiver
			assert.Equal(t, eventID1, event.EventID)
			event, err = ltngqueue.Ack(ctx, event)
			require.NoError(t, err)

			return nil, nil
		})

		time.Sleep(50 * time.Millisecond)
		op.OpX(func() (any, error) {
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
			event := <-receiver
			assert.Equal(t, eventID2, event.EventID)
			event, err = ltngqueue.Ack(ctx, event)
			require.NoError(t, err)

			return nil, nil
		})
		err = op.WaitAndWrapErr()
		require.NoError(t, err)

		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("successfully test fan out propagation", func(t *testing.T) {
		//
	})

	t.Run("successfully test round robin group propagation", func(t *testing.T) {
		//
	})

	t.Run("successfully test fan out group propagation", func(t *testing.T) {
		//
	})

	t.Run("successfully test round robin and fan out group propagation", func(t *testing.T) {
		//
	})
}

func TestReadFromFQ(t *testing.T) {
	ctx := context.Background()
	fq, err := filequeuev1.New(ctx,
		"test/round_robin", "test_round_robin")
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read(ctx)
		if err != nil {
			t.Log(err)
			break
		}

		err = fq.Pop(ctx)
		assert.NoError(t, err)

		counter++
	}
	t.Log(counter)
}

// TODOa
// in order to fix the queue:
// Shift the reader cursor from the popped items.
// // don't reset the reader cursor, but if a item is removed, update the cursor accordingly
// perhaps
