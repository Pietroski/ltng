package v2

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

func newLTNGEngine(
	ctx context.Context, opts ...options.Option,
) (*LTNGEngine, error) {
	fq, err := filequeuev1.New(ctx,
		filequeuev1.GenericFileQueueFilePath,
		filequeuev1.GenericFileQueueFileName,
	)
	if err != nil {
		return nil, err
	}

	engine := &LTNGEngine{
		opMtx:                  lock.NewEngineLock(),
		mtx:                    new(sync.RWMutex),
		fq:                     fq,
		fileManager:            rw.NewFileManager(ctx),
		memoryStore:            memorystorev1.New(ctx),
		storeFileMapping:       safe.NewGenericMap[*ltngenginemodels.FileInfo](),
		itemFileMapping:        safe.NewGenericMap[*ltngenginemodels.FileInfo](),
		markedAsDeletedMapping: safe.NewGenericMap[struct{}](),
		serializer:             serializer.NewRawBinarySerializer(),
	}
	options.ApplyOptions(engine, opts...)

	if err = engine.init(ctx); err != nil {
		return nil, err
	}

	//engine.opSaga = newOpSaga(ctx, engine)
	return engine, nil
}

func (e *LTNGEngine) init(ctx context.Context) error {
	if err := e.createStatsPathOnDisk(ctx); err != nil {
		return err
	}
	if _, err := e.createOrOpenRelationalStatsStoreOnDisk(ctx); err != nil {
		return fmt.Errorf("failed creating store stats manager on-disk: %w", err)
	}
	if err := os.MkdirAll(
		ltngenginemodels.DBTmpDelDataPath, ltngenginemodels.DBFilePerm,
	); err != nil {
		return err
	}
	if err := os.MkdirAll(
		ltngenginemodels.DBTmpDelStatsPath, ltngenginemodels.DBFilePerm,
	); err != nil {
		return err
	}

	if err := e.fq.Init(); err != nil {
		return fmt.Errorf("failed initializing file queue: %w", err)
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
		func(key string, value *ltngenginemodels.FileInfo) bool {
			if !rw.IsFileClosed(value.File) {
				if err := value.File.Close(); err != nil {
					log.Printf("error closing file from %s store: %v\n", key, err)
				}
			}

			return true
		})
}

func (e *LTNGEngine) closeItems() {
	for !e.fq.CheckAndClose() {
		runtime.Gosched()
	}

	for e.opSaga.pidRegister.CountNumber() != 0 {
		runtime.Gosched()
	}

	e.itemFileMapping.RangeAndDelete(func(key string, value *ltngenginemodels.FileInfo) bool {
		if !rw.IsFileClosed(value.File) {
			if err := value.File.Close(); err != nil {
				log.Printf("error closing file from %s item store: %v\n", key, err)
			}
		}

		return true
	})

	e.opSaga.Close()
}
