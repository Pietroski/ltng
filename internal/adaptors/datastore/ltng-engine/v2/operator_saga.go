package v2

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/process"
)

func ResponseAccumulator(respSigChan ...chan error) error {
	var err error
	op := syncx.NewThreadOperator("responseAccumulator")
	for _, sigChan := range respSigChan {
		op.OpX(func() (any, error) {
			sigErr := <-sigChan
			if sigErr != nil {
				return nil, sigErr
			}

			return nil, nil
		})
	}
	if err = op.WaitAndWrapErr(); err != nil {
		return errorsx.Wrap(err, "responseAccumulator")
	}

	return err
}

// #####################################################################################################################

const threadLimit = 1 << 6

type opSaga struct {
	e            *LTNGEngine
	fq           *filequeuev1.FileQueue
	cancel       context.CancelFunc
	offThread    *syncx.OffThread
	crudChannels *ltngenginemodels.CrudChannels
	pidRegister  *process.Register
}

func newOpSaga(ctx context.Context, e *LTNGEngine) *opSaga {
	ctx, cancel := context.WithCancel(ctx)
	op := &opSaga{
		e:         e,
		fq:        e.fq,
		cancel:    cancel,
		offThread: syncx.NewThreadOperator("OpSaga", syncx.WithThreadLimit(threadLimit)),
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

	newCreateSaga(ctx, op)
	newUpsertSaga(ctx, op)
	newDeleteSaga(ctx, op)

	return op
}

func (op *opSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_saga-ListenAndTrigger")
	// TODO: we need to be able to close the channel to stop the loop
	loop.RunFromChannel(ctx,
		op.crudChannels.OpSagaChannel.QueueChannel.Ch,
		func(_ struct{}) {
			op.listenAndTrigger(ctx)
		},
	)
	op.cancel()
}

func (op *opSaga) listenAndTrigger(ctx context.Context) {
	op.pidRegister.Count()
	bs, err := op.fq.ReadFromCursor(ctx)
	if err != nil {
		op.e.logger.Error(ctx, "error reading item from file queue", "error", err)
		return
	}

	var itemInfoData ltngenginemodels.ItemInfoData
	if err = op.e.serializer.Deserialize(bs, &itemInfoData); err != nil {
		op.e.logger.Error(ctx, "error deserializing item info data from file queue", "error", err)
		return
	}

	respSignalChan := make(chan error)
	itemInfoData.RespSignal = respSignalChan
	itemInfoData.Ctx = context.Background()

	switch itemInfoData.OpType {
	case ltngenginemodels.OpTypeCreate:
		op.crudChannels.CreateChannels.InfoChannel.Send(&itemInfoData)
	case ltngenginemodels.OpTypeUpsert:
		op.crudChannels.UpsertChannels.InfoChannel.Send(&itemInfoData)
	case ltngenginemodels.OpTypeDelete:
		op.crudChannels.DeleteChannels.InfoChannel.Send(&itemInfoData)
	default:
		op.e.logger.Error(ctx, "unknown op type", "op_type", itemInfoData.OpType)
		return
	}

	if err = ResponseAccumulator(respSignalChan); err != nil {
		op.e.logger.Error(ctx, "error accumulating item info data from file queue on op type",
			"op_type", itemInfoData.OpType, "error", err)
	}
	op.pidRegister.CountEnd()
}

func (op *opSaga) Close() {
	op.crudChannels.OpSagaChannel.Close()
	op.crudChannels.CreateChannels.Close()
	op.crudChannels.UpsertChannels.Close()
	op.crudChannels.DeleteChannels.Close()

	op.cancel()
	op.offThread.Wait()
}

// #####################################################################################################################
