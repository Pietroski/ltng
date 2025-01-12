package v2

import (
	"context"
	"fmt"
	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/process"
	"log"
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

type opSaga struct {
	e            *LTNGEngine
	fq           *filequeuev1.FileQueue
	crudChannels *ltngenginemodels.CrudChannels
	pidRegister  *process.Register
}

func newOpSaga(ctx context.Context, e *LTNGEngine) *opSaga {
	op := &opSaga{
		e:  e,
		fq: e.fq,
		crudChannels: &ltngenginemodels.CrudChannels{
			OpSagaChannel:  ltngenginemodels.MakeOpChannels(),
			CreateChannels: ltngenginemodels.MakeOpChannels(),
			UpsertChannels: ltngenginemodels.MakeOpChannels(),
			DeleteChannels: ltngenginemodels.MakeOpChannels(),
		},
		pidRegister: process.New(ctx),
	}

	go op.ListenAndTrigger(ctx)

	newCreateSaga(ctx, op)
	newUpsertSaga(ctx, op)
	newDeleteSaga(ctx, op)

	return op
}

func (op *opSaga) ListenAndTrigger(ctx context.Context) {
	for _ = range op.crudChannels.OpSagaChannel.QueueChannel {
		op.pidRegister.Count()
		bs, err := op.fq.ReadFromCursor(ctx)
		if err != nil {
			log.Printf("error reading item from file queue: %v\n", err)
			continue
		}

		var itemInfoData ltngenginemodels.ItemInfoData
		if err = op.e.serializer.Deserialize(bs, &itemInfoData); err != nil {
			log.Printf("error deserializing item info data from file queue: %v", err)
			continue
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
			continue
		}

		if err = ResponseAccumulator(respSignalChan); err != nil {
			log.Printf("error accumulating item info data from file queue: %v", err)
		}
		op.pidRegister.CountEnd()
	}
}

// #####################################################################################################################
