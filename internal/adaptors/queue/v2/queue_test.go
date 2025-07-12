package ltngqueue_engine

import (
	"context"
	"testing"

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
	t.Run("Flow", func(t *testing.T) {
		Test_DeleteTestFileQueue(t)

		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",
		}
		qp, err := ltngqueue.CreateQueuePublisher(ctx, queue)
		require.NoError(t, err)
		_ = qp

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
		cancel()

		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("Flow", func(t *testing.T) {
		Test_DeleteTestFileQueue(t)

		ctx, cancel := context.WithCancel(context.Background())

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		queue := &queuemodels.Queue{
			Name: "test-queue",
			Path: "test/queue",
		}
		qp, err := ltngqueue.CreateQueuePublisher(ctx, queue)
		require.NoError(t, err)
		_ = qp

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
