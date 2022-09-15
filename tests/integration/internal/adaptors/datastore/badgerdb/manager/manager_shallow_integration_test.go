package manager

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
)

func Test_Integration_CreateStore(t *testing.T) {
	t.Run(
		"happy path",
		func(t *testing.T) {
			var err error
			ctx, _ := context.WithCancel(context.Background())
			ctx, err = go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)

			logger := go_logger.FromCtx(ctx)

			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()
			badgerManager := manager.NewBadgerLocalManager(db, serializer, logger)

			err = badgerManager.Start()
			require.NoError(t, err)

			stores, err := badgerManager.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Debugf(
				"stores",
				go_logger.Field{"stores": stores},
			)

			info := &management_models.DBInfo{
				Name:         "integration-manager-test",
				Path:         "integration-manager-test/path-1",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}

			err = badgerManager.CreateStore(ctx, info)
			require.NoError(t, err)

			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Debugf(
				"stores",
				go_logger.Field{"stores": stores},
			)

			err = badgerManager.DeleteStore(ctx, info.Name)
			require.NoError(t, err)

			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Debugf(
				"stores",
				go_logger.Field{"stores": stores},
			)
		},
	)
}
