package ltngqueue_engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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
		ctx, cancel := context.WithCancel(context.Background())
		_ = cancel

		ltngqueue, err := New(ctx)
		require.NoError(t, err)

		err = ltngqueue.Close()
		require.NoError(t, err)
	})

	t.Run("queue creation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		_ = cancel

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

	//t.Run("flow", func(t *testing.T) {
	//	ctx, cancel := context.WithCancel(context.Background())
	//	_ = cancel
	//
	//	ltngqueue, err := New(ctx)
	//	require.NoError(t, err)
	//
	//	queue := &queuemodels.Queue{
	//		Name: "test-queue",
	//		Path: "test/queue",
	//
	//		ConsumerCountLimit: 5,
	//	}
	//	qp, err := ltngqueue.CreateQueue(ctx, queue)
	//	require.NoError(t, err)
	//	_ = qp
	//
	//	eventCount := 10
	//	for i := 0; i < eventCount; i++ {
	//		gtd := &GenericTestData{
	//			FieldString: go_random.RandomString(12),
	//			FieldInt:    int(go_random.RandomInt(1, 100)),
	//			FieldBool:   true,
	//		}
	//		bs, err := ltngqueue.serializer.Serialize(gtd)
	//		require.NoError(t, err)
	//
	//		metadata := &queuemodels.EventMetadata{
	//			Metadata:       nil,
	//			RetryCount:     0,
	//			SentAt:         0,
	//			ReceivedAt:     0,
	//			ReceivedAtList: nil,
	//		}
	//		event := &queuemodels.Event{
	//			EventID:  "",
	//			Queue:    queue,
	//			Data:     bs,
	//			Metadata: metadata,
	//		}
	//
	//		e, err := ltngqueue.Publish(ctx, event)
	//		require.NoError(t, err)
	//		require.EqualValues(t, event, e)
	//	}
	//	time.Sleep(5 * time.Second)
	//
	//	//{
	//	//	nodeUUID, err := uuid.NewRandom()
	//	//	require.NoError(t, err)
	//	//	nodeID := queue.GetCompleteLockKey() + "_" + nodeUUID.String()
	//	//
	//	//	receiver := make(chan *queuemodels.Event, 1)
	//	//	publisher := &queuemodels.Publisher{
	//	//		NodeID: nodeID,
	//	//		Sender: receiver,
	//	//	}
	//	//	err = ltngqueue.SubscribeToQueue(ctx, queue, publisher)
	//	//	require.NoError(t, err)
	//	//
	//	//	var count int
	//	//	for {
	//	//		//if count > eventCount {
	//	//		//	break
	//	//		//}
	//	//
	//	//		select {
	//	//		case event := <-receiver:
	//	//			count++
	//	//			t.Log(event)
	//	//
	//	//			if count > eventCount {
	//	//				break
	//	//			}
	//	//		}
	//	//	}
	//	//}
	//
	//	cancel()
	//	err = ltngqueue.Close()
	//	require.NoError(t, err)
	//
	//	//Test_DeleteTestFileQueue(t)
	//})
}

func Test_DeleteTestFileQueue(t *testing.T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngDBBasePath)
	require.NoError(t, err)
	_, err = execx.DelHardExec(ctx, ltngFileQueueBasePath)
	require.NoError(t, err)
}
