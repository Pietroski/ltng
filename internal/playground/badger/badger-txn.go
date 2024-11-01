package main

import (
	"context"
	"log"
	"os"

	"github.com/dgraph-io/badger/v3"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
)

func main() {
	ctx, _ := context.WithCancel(context.Background())
	tracer := go_tracer.NewCtxTracer()

	var err error
	ctx, err = tracer.Trace(ctx)
	if err != nil {
		log.Fatalf("failed to create trace id: %v", err)
		os.Exit(1)
		return
	}

	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: false,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)
	logger = logger.FromCtx(ctx)

	logger.Debugf("test here")

	db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement + "playground-test"))
	handleErr(err)
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte("test-1"))
	})
	logger.Debugf("delete anyways", go_logger.Field{"err": err})

	txn := db.NewTransaction(true)

	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("test-1"))
		if err != nil {
			logger.Errorf("err getting key", go_logger.Field{"err": err})
			return err
		}

		bs, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		logger.Debugf(
			"badger 1 read", go_logger.Field{"item": string(bs)},
		)

		return nil
	})
	err = nil

	err = txn.Set([]byte("test-1"), []byte("value-test-1"))
	handleErr(err)
	txn.Commit()

	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("test-1"))
		if err != nil {
			logger.Errorf("err getting key", go_logger.Field{"err": err})
			return err
		}

		bs, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		logger.Debugf(
			"badger 2 read", go_logger.Field{"item": string(bs)},
		)

		return nil
	})
	err = nil

	item, err := txn.Get([]byte("test-1"))
	if err != nil {
		logger.Errorf("err getting key from outside", go_logger.Field{"err": err})
	} else {
		err = nil

		bs, _ := item.ValueCopy(nil)
		logger.Debugf(
			"badger outside item read", go_logger.Field{"item": string(bs)},
		)
		txn.Discard()
	}
	txn.Delete([]byte("test-1"))
	txn.ReadTs()

	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("test-1"))
		if err != nil {
			logger.Errorf("err getting key", go_logger.Field{"err": err})
			return err
		}

		bs, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		logger.Debugf(
			"badger last read", go_logger.Field{"item": string(bs)},
		)

		return nil
	})

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte("test-1"))
	})
	logger.Debugf("delete anyways", go_logger.Field{"err": err})
}

func handleErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
