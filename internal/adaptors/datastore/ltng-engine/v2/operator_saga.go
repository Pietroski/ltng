package v2

import (
	"context"
	"fmt"
	"log"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/process"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/ctxrunner"
)

func ResponseAccumulator(respSigChan ...chan error) error {
	var err error
	for _, sigChan := range respSigChan {
		sigErr := <-sigChan
		if sigErr != nil {
			if err == nil {
				err = sigErr
			} else {
				err = fmt.Errorf("%s: %w", sigErr, err)
			}
		}
	}

	return err
}

// #####################################################################################################################

const threadLimit = 1 << 6

type opSaga struct {
	e            *LTNGEngine
	fq           *filequeuev1.FileQueue
	cancel       context.CancelFunc
	offThread    *concurrent.OffThread
	crudChannels *ltngenginemodels.CrudChannels
	pidRegister  *process.Register
}

func newOpSaga(ctx context.Context, e *LTNGEngine) *opSaga {
	ctx, cancel := context.WithCancel(ctx)
	op := &opSaga{
		e:         e,
		fq:        e.fq,
		cancel:    cancel,
		offThread: concurrent.New("OpSaga", concurrent.WithThreadLimit(threadLimit)),
		crudChannels: &ltngenginemodels.CrudChannels{
			OpSagaChannel:  ltngenginemodels.MakeOpChannels(),
			CreateChannels: ltngenginemodels.MakeOpChannels(),
			UpsertChannels: ltngenginemodels.MakeOpChannels(),
			DeleteChannels: ltngenginemodels.MakeOpChannels(),
		},
		pidRegister: process.New(ctx),
	}

	op.offThread.Op(func() {
		op.ListenAndTrigger(ctx)
	})

	// go op.ListenAndTrigger(ctx)

	newCreateSaga(ctx, op)
	newUpsertSaga(ctx, op)
	newDeleteSaga(ctx, op)

	return op
}

func (op *opSaga) ListenAndTrigger(ctx context.Context) {
	ctxrunner.RunWithCancellation(ctx,
		op.crudChannels.OpSagaChannel.QueueChannel,
		func(_ struct{}) {
			op.listenAndTrigger(ctx)
		},
	)
	op.cancel() // TODO: not necessary?

	//for {
	//	select {
	//	case <-ctx.Done():
	//		log.Printf("context done: %v\n", ctx.Err())
	//		op.crudChannels.OpSagaChannel.Close()
	//		for _ = range op.crudChannels.OpSagaChannel.QueueChannel {
	//			op.listenAndTrigger(ctx)
	//		}
	//		log.Println("killing goroutines - opSaga")
	//
	//		//op.crudChannels.CreateChannels.Close()
	//		//op.crudChannels.UpsertChannels.Close()
	//		//op.crudChannels.DeleteChannels.Close()
	//
	//		return
	//	case _ = <-op.crudChannels.OpSagaChannel.QueueChannel:
	//		op.listenAndTrigger(ctx)
	//	}
	//}

	//for _ = range op.crudChannels.OpSagaChannel.QueueChannel {
	//	op.pidRegister.Count()
	//	bs, err := op.fq.ReadFromCursor(ctx)
	//	if err != nil {
	//		log.Printf("error reading item from file queue: %v\n", err)
	//		continue
	//	}
	//
	//	var itemInfoData ltngenginemodels.ItemInfoData
	//	if err = op.e.serializer.Deserialize(bs, &itemInfoData); err != nil {
	//		log.Printf("error deserializing item info data from file queue: %v", err)
	//		continue
	//	}
	//
	//	respSignalChan := make(chan error)
	//	itemInfoData.RespSignal = respSignalChan
	//	itemInfoData.Ctx = context.Background()
	//
	//	switch itemInfoData.OpType {
	//	case ltngenginemodels.OpTypeCreate:
	//		op.crudChannels.CreateChannels.InfoChannel <- &itemInfoData
	//	case ltngenginemodels.OpTypeUpsert:
	//		op.crudChannels.UpsertChannels.InfoChannel <- &itemInfoData
	//	case ltngenginemodels.OpTypeDelete:
	//		op.crudChannels.DeleteChannels.InfoChannel <- &itemInfoData
	//	default:
	//		log.Printf("unknown op type: %v", itemInfoData.OpType)
	//		continue
	//	}
	//
	//	if err = ResponseAccumulator(respSignalChan); err != nil {
	//		log.Printf("error accumulating item info data from file queue: %v", err)
	//	}
	//	op.pidRegister.CountEnd()
	//}
}

func (op *opSaga) listenAndTrigger(ctx context.Context) {
	op.pidRegister.Count()
	bs, err := op.fq.ReadFromCursor(ctx)
	if err != nil {
		log.Printf("error reading item from file queue: %v\n", err)
		return
	}

	var itemInfoData ltngenginemodels.ItemInfoData
	if err = op.e.serializer.Deserialize(bs, &itemInfoData); err != nil {
		log.Printf("error deserializing item info data from file queue: %v", err)
		return
	}

	respSignalChan := make(chan error)
	itemInfoData.RespSignal = respSignalChan
	itemInfoData.Ctx = context.Background()

	switch itemInfoData.OpType {
	case ltngenginemodels.OpTypeCreate:
		op.crudChannels.CreateChannels.InfoChannel <- &itemInfoData
	case ltngenginemodels.OpTypeUpsert:
		op.crudChannels.UpsertChannels.InfoChannel <- &itemInfoData
	case ltngenginemodels.OpTypeDelete:
		op.crudChannels.DeleteChannels.InfoChannel <- &itemInfoData
	default:
		log.Printf("unknown op type: %v", itemInfoData.OpType)
		return
	}

	if err = ResponseAccumulator(respSignalChan); err != nil {
		log.Printf("error accumulating item info data from file queue: %v", err)
	}
	op.pidRegister.CountEnd()
}

func (op *opSaga) Close() {
	op.cancel()
	op.offThread.Wait()
}

// #####################################################################################################################
