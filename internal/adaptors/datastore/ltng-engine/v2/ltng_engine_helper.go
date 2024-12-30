package v2

import (
	"context"
	"fmt"
	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	concurrentv1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1/concurrent"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	"log"
	"os"
	"runtime"
	"sync"
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
		opMtx:            lock.NewEngineLock(),
		mtx:              new(sync.Mutex),
		fq:               fq,
		fileManager:      rw.NewFileManager(ctx),
		memoryStore:      memorystorev1.New(ctx),
		caching:          concurrentv1.New(ctx),
		storeFileMapping: make(map[string]*ltngenginemodels.FileInfo),
		itemFileMapping:  make(map[string]*ltngenginemodels.FileInfo),
		serializer:       serializer.NewRawBinarySerializer(),
	}
	options.ApplyOptions(engine, opts...)

	if err = engine.init(ctx); err != nil {
		return nil, err
	}

	op := newOpSaga(ctx, engine)

	engine.opSaga = op
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

	return nil
}

func (e *LTNGEngine) close() {
	e.closeItems()
	e.closeStores()
}

func (e *LTNGEngine) closeStores() {
	for k, v := range e.storeFileMapping {
		e.opMtx.Lock(k, v.FileData.Header.StoreInfo)
		if err := v.File.Close(); err != nil {
			log.Printf("error closing file from %s store: %v\n", k, err)
		}
		delete(e.storeFileMapping, k)
		e.opMtx.Unlock(k)
	}
}

func (e *LTNGEngine) closeItems() {
	//isEmpty, err := e.fq.IsEmpty()
	//if err == nil {
	//	for !isEmpty {
	//		isEmpty, err = e.fq.IsEmpty()
	//		if err != nil {
	//			break
	//		}
	//	}
	//}

	for !e.fq.CheckAndClose() {
		runtime.Gosched()
	}

	//time.Sleep(500 * time.Millisecond)

	//for k, v := range e.itemFileMapping {
	//	e.opMtx.Lock(k, v.FileData.Header.StoreInfo)
	//	if err := v.File.Close(); err != nil {
	//		log.Printf("error closing file from %s item store: %v\n", k, err)
	//	}
	//	delete(e.itemFileMapping, k)
	//	e.opMtx.Unlock(k)
	//}
}
