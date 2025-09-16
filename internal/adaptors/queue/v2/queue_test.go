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
	Test_DeleteTestFileQueue(t)

	t.Run("queue instantiation", func(t *testing.T) {
		ctx := context.Background()

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("queue creation", func(t *testing.T) {
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
		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx)
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
		time.Sleep(1 * time.Second)

		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("subscription / consumption flow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 5,
		}
		_, err = ltngqueue.CreateQueue(ctx, queue)
		require.NoError(t, err)

		{
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
		}

		time.Sleep(1 * time.Second)

		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("publish / consumption flow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",

			ConsumerCountLimit: 10,
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

		time.Sleep(2 * time.Second)
		assert.EqualValues(t, uint64(eventCount), count.Load())
		cancel()
		err = ltngqueue.Close()
		require.NoError(t, err)
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

func Test_DeleteTestFileQueue(t *testing.T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngDBBasePath)
	require.NoError(t, err)
	_, err = execx.DelHardExec(ctx, ltngFileQueueBasePath)
	require.NoError(t, err)
}
