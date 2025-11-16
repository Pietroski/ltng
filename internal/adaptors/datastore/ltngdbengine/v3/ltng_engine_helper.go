package ltngdbenginev3

import (
	"context"
	"runtime"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"

	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func newLTNGEngine(
	ctx context.Context, opts ...options.Option,
) (*LTNGEngine, error) {
	ctx, cancel := context.WithCancel(ctx)
	fqCtx, cancelFq := context.WithCancel(ctx)

	fq, err := mmap.NewFileQueue(fileiomodels.GetFileQueueFilePath(
		fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath,
		fileiomodels.FQ,
	))
	if err != nil {
		cancel()
		cancelFq()
		return nil, err
	}

	engine := &LTNGEngine{
		ctx:      ctx,
		cancel:   cancel,
		fqCtx:    fqCtx,
		cancelFq: cancelFq,

		kvLock: syncx.NewKVLock(),
		mtx:    new(sync.RWMutex),

		fq:                         fq,
		memoryStore:                memorystorev1.New(ctx),
		storeFileMapping:           syncx.NewGenericMap[*ltngdbenginemodelsv3.FileInfo](),
		itemFileMapping:            syncx.NewGenericMap[*ltngdbenginemodelsv3.FileInfo](),
		relationalStoreFileMapping: syncx.NewGenericMap[*ltngdbenginemodelsv3.RelationalFileInfo](),
		relationalItemFileMapping:  syncx.NewGenericMap[*ltngdbenginemodelsv3.RelationalFileInfo](),
		markedAsDeletedMapping:     syncx.NewGenericMap[struct{}](),

		serializer: serializer.NewRawBinarySerializer(),
		logger:     slogx.New(),
		tracer:     tracer.New(),

		mngrStoreInfo: ltngdbenginemodelsv3.DBManagerStoreInfo,
	}
	options.ApplyOptions(engine, opts...)

	if err = engine.init(ctx); err != nil {
		return nil, err
	}

	return engine, nil
}

func (e *LTNGEngine) init(ctx context.Context) error {
	if err := e.createStatsPathsOnDisk(ctx, e.mngrStoreInfo); err != nil {
		return errorsx.Wrap(err, "failed initializing file queue")
	}

	//if err := e.createDataPathsOnDisk(ctx, e.mngrStoreInfo); err != nil {
	//	return errorsx.Wrap(err, "failed initializing file queue")
	//}

	if _, err := e.createOpenRelationalStatsStoreOnDisk(ctx); err != nil {
		return err
	}

	if _, err := e.createStoreOnDisk(ctx, e.mngrStoreInfo); err != nil {
		if !errorsx.Is(err, mmap.ErrFileCannotBeOverWritten) {
			return err
		}
	}

	e.opSaga = newOpSaga(ctx, e)

	return nil
}

func (e *LTNGEngine) close() {
	e.closeItems()
	e.closeStores()
}

func (e *LTNGEngine) closeStores() {
	e.storeFileMapping.RangeAndDelete(
		func(fileStore string, value *ltngdbenginemodelsv3.FileInfo) bool {
			if !osx.IsFileClosed(value.File) {
				if err := value.File.Close(); err != nil {
					e.logger.Error(e.ctx, "error closing file from store",
						"file_store", fileStore, "err", err)
				}
			}

			return true
		})
}

func (e *LTNGEngine) closeItems() {
	e.cancelFq()
	if err := e.fq.Close(); err != nil {
		e.logger.Error(e.ctx, "error closing file queue", "err", err)
	}

	for e.opSaga.pidRegister.CountNumber() != 0 {
		runtime.Gosched()
	}

	e.itemFileMapping.RangeAndDelete(func(fileItem string, value *ltngdbenginemodelsv3.FileInfo) bool {
		if !osx.IsFileClosed(value.File) {
			if err := value.File.Close(); err != nil {
				e.logger.Error(e.ctx, "error closing file from item store",
					"file_item", fileItem, "err", err)
			}
		}

		return true
	})

	e.opSaga.Close()
}

func (e *LTNGEngine) getTracerID(ctx context.Context) (string, error) {
	_, ctxInfo, err := e.tracer.TraceInfo(ctx)
	if err != nil {
		return "", err
	}

	return ctxInfo.ID.String(), nil
}
