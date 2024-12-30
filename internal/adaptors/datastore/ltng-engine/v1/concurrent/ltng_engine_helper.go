package v1

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
)

const (
	opChanLimit = 100
)

func newLTNGEngine(
	ctx context.Context, opts ...options.Option,
) (*LTNGEngine, error) {
	engine := &LTNGEngine{
		opMtx:            lock.NewEngineLock(),
		mtx:              new(sync.Mutex),
		serializer:       serializer.NewRawBinarySerializer(),
		storeFileMapping: make(map[string]*fileInfo),
		itemFileMapping:  make(map[string]*fileInfo),

		mainThreadOperatorChan:               make(chan *InputInfoData, opChanLimit),
		mainThreadOperatorRollbackChan:       make(chan *InputInfoData, opChanLimit),
		indexThreadOperatorChan:              make(chan *InputInfoData, opChanLimit),
		indexThreadOperatorRollbackChan:      make(chan *InputInfoData, opChanLimit),
		indexListThreadOperatorChan:          make(chan *InputInfoData, opChanLimit),
		indexListThreadOperatorRollbackChan:  make(chan *InputInfoData, opChanLimit),
		relationalThreadOperatorChan:         make(chan *InputInfoData, opChanLimit),
		relationalThreadOperatorRollbackChan: make(chan *InputInfoData, opChanLimit),
	}
	options.ApplyOptions(engine, opts...)

	if err := engine.init(ctx); err != nil {
		return nil, err
	}

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
		dbTmpDelDataPath, dbFilePerm,
	); err != nil {
		return err
	}
	if err := os.MkdirAll(
		dbTmpDelStatsPath, dbFilePerm,
	); err != nil {
		return err
	}

	e.initThreadOperators(ctx)

	return nil
}

func (e *LTNGEngine) initThreadOperators(ctx context.Context) {
	cis := newCreateSaga(e)
	go cis.cidOnThread(ctx)
	go cis.ciidOnThread(ctx)
	go cis.ciildOnThread(ctx)
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
	for k, v := range e.itemFileMapping {
		e.opMtx.Lock(k, v.FileData.Header.StoreInfo)
		if err := v.File.Close(); err != nil {
			log.Printf("error closing file from %s store: %v\n", k, err)
		}
		delete(e.itemFileMapping, k)
		e.opMtx.Unlock(k)
	}
}
