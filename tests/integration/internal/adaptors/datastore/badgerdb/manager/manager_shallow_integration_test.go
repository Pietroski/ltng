package manager

//import (
//	"context"
//	"testing"
//	"time"
//
//	"github.com/dgraph-io/badger/v3"
//	"github.com/stretchr/testify/require"
//
//	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
//	gitlab.com/pietroski-software-company/devex/golang/serializer
//	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"
//
//	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/manager"
//	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
//)
//
//func Test_Integration_CreateStore(t *testing.T) {
//	t.Run(
//		"happy path",
//		func(t *testing.T) {
//			var err error
//			ctx, _ := context.WithCancel(context.Background())
//			ctx, err = go_tracer.NewCtxTracer().Trace(ctx)
//			require.NoError(t, err)
//
//			logger := go_logger.FromCtx(ctx)
//
//			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
//			require.NoError(t, err)
//
//			serializer := go_serializer.NewJsonSerializer()
//
//			params := &badgerdb_manager_adaptor_v4.BadgerLocalManagerV3Params{
//				DB:         db,
//				Logger:     logger,
//				Serializer: serializer,
//			}
//			badgerManager, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV3(params)
//			require.NoError(t, err)
//			defer func() {
//				badgerManager.ShutdownStores()
//				badgerManager.Shutdown()
//			}()
//
//			err = badgerManager.Start()
//			require.NoError(t, err)
//
//			stores, err := badgerManager.ListStoreInfo(ctx, 0, 0)
//			require.NoError(t, err)
//			logger.Debugf(
//				"stores",
//				go_logger.Field{"stores": stores},
//			)
//
//			info := &badgerdb_management_models_v3.DBInfo{
//				Name:         "integration-manager-test",
//				Path:         "integration-manager-test/path-1",
//				CreatedAt:    time.Now(),
//				LastOpenedAt: time.Now(),
//			}
//
//			err = badgerManager.CreateStore(ctx, info)
//			require.NoError(t, err)
//
//			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
//			require.NoError(t, err)
//			logger.Debugf(
//				"stores",
//				go_logger.Field{"stores": stores},
//			)
//
//			err = badgerManager.DeleteStore(ctx, info.Name)
//			require.NoError(t, err)
//
//			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
//			require.NoError(t, err)
//			logger.Debugf(
//				"stores",
//				go_logger.Field{"stores": stores},
//			)
//		},
//	)
//
//	t.Run(
//		"happy path",
//		func(t *testing.T) {
//			var err error
//			ctx, _ := context.WithCancel(context.Background())
//			ctx, err = go_tracer.NewCtxTracer().Trace(ctx)
//			require.NoError(t, err)
//
//			logger := go_logger.FromCtx(ctx)
//
//			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
//			require.NoError(t, err)
//
//			serializer := go_serializer.NewJsonSerializer()
//			params := &badgerdb_manager_adaptor_v4.BadgerLocalManagerV4Params{
//				DB:         db,
//				Logger:     logger,
//				Serializer: serializer,
//			}
//			badgerManager, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(params)
//			require.NoError(t, err)
//			defer func() {
//				badgerManager.ShutdownStores()
//				badgerManager.Shutdown()
//			}()
//
//			err = badgerManager.Start()
//			require.NoError(t, err)
//
//			stores, err := badgerManager.ListStoreInfo(ctx, 0, 0)
//			require.NoError(t, err)
//			logger.Debugf(
//				"stores",
//				go_logger.Field{"stores": stores},
//			)
//
//			info := &badgerdb_management_models_v3.DBInfo{
//				Name:         "integration-manager-test",
//				Path:         "integration-manager-test/path-1",
//				CreatedAt:    time.Now(),
//				LastOpenedAt: time.Now(),
//			}
//
//			err = badgerManager.DeleteStore(ctx, info.Name)
//			require.Error(t, err)
//
//			err = badgerManager.CreateStore(ctx, info)
//			require.NoError(t, err)
//
//			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
//			require.NoError(t, err)
//			logger.Debugf(
//				"stores",
//				go_logger.Field{"stores": stores},
//			)
//
//			err = badgerManager.DeleteStore(ctx, info.Name)
//			require.NoError(t, err)
//
//			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
//			require.NoError(t, err)
//			logger.Debugf(
//				"stores",
//				go_logger.Field{"stores": stores},
//			)
//		},
//	)
//}
