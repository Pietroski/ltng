package ltngdbenginev3

import (
	"context"
	"errors"
	"io"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/process"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
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
	fq           *mmap.FileQueue
	cancel       context.CancelFunc
	offThread    *syncx.OffThread
	crudChannels *ltngdbenginemodelsv3.CrudChannels
	pidRegister  *process.Register
}

func newOpSaga(ctx context.Context, e *LTNGEngine) *opSaga {
	ctx, cancel := context.WithCancel(ctx)
	op := &opSaga{
		e:         e,
		fq:        e.fq,
		cancel:    cancel,
		offThread: syncx.NewThreadOperator("OpSaga", syncx.WithThreadLimit(threadLimit)),
		crudChannels: &ltngdbenginemodelsv3.CrudChannels{
			OpSagaChannel:  ltngdbenginemodelsv3.MakeOpChannels(),
			CreateChannels: ltngdbenginemodelsv3.MakeOpChannels(),
			UpsertChannels: ltngdbenginemodelsv3.MakeOpChannels(),
			DeleteChannels: ltngdbenginemodelsv3.MakeOpChannels(),
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
	loop.Run(op.e.fqCtx, func() error {
		return op.listenAndTrigger(ctx)
	})
}

func (op *opSaga) listenAndTrigger(ctx context.Context) error {
	if op.e.fqCtx.Err() != nil {
		return loop.ErrEnded
	}

	op.pidRegister.Count()
	defer op.pidRegister.CountEnd()

	bs, err := op.fq.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}

		op.e.logger.Error(ctx, "error reading item from file queue", "error", err)
		return err
	}

	// TODO: change it for a item info wrapper after this...
	var itemInfoData ltngdbenginemodelsv3.ItemInfoData
	if err = op.e.serializer.Deserialize(bs, &itemInfoData); err != nil {
		op.e.logger.Error(ctx, "error deserializing item info data from file queue", "error", err)
		return err
	}

	ctx, err = op.e.tracer.WithRawUUID(context.Background(), itemInfoData.TraceID)
	if err != nil {
		op.e.logger.Error(ctx, "error tracing item info data from file queue", "error", err)
		return err
	}

	respSignalChan := make(chan error)
	itemInfoData.RespSignal = respSignalChan
	itemInfoData.Ctx = ctx

	switch itemInfoData.OpType {
	case ltngdbenginemodelsv3.OpTypeCreate:
		op.crudChannels.CreateChannels.InfoChannel.Send(&itemInfoData)
	case ltngdbenginemodelsv3.OpTypeUpsert:
		op.crudChannels.UpsertChannels.InfoChannel.Send(&itemInfoData)
	case ltngdbenginemodelsv3.OpTypeDelete:
		op.crudChannels.DeleteChannels.InfoChannel.Send(&itemInfoData)
	default:
		op.e.logger.Error(ctx, "unknown op type", "op_type", itemInfoData.OpType)
		return errorsx.Errorf("unknown op type: %s", itemInfoData.OpType)
	}

	if err = ResponseAccumulator(respSignalChan); err != nil {
		op.e.logger.Error(ctx, "error accumulating item info data from file queue on op type",
			"op_type", itemInfoData.OpType, "error", err)
	}

	return nil
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
