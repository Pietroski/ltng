package badgerdb_indexed_operator_controller

import (
	"context"
	"fmt"
	mock_indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed/mocks"
	mock_indexed_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations/indexed/mocks"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	mock_go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

func TestBadgerDBServiceController_Set(t *testing.T) {
	t.Run(
		"error getting db info from memory or disk",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockIndexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			mockIndexedOperator := mock_indexed_operator.NewMockIndexedOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			indexedOperator := NewBadgerDBIndexerOperatorServiceController(
				logger, mockBinder, mockIndexedManager, mockIndexedOperator)

			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.SetIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Item: &grpc_ops.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
			}

			// build stubs
			mockIndexedManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(&management_models.DBMemoryInfo{}, fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedSet(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
		},
	)

	t.Run(
		"error operating on giving database",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockIndexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			mockIndexedOperator := mock_indexed_operator.NewMockIndexedOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			indexedOperator := NewBadgerDBIndexerOperatorServiceController(
				logger, mockBinder, mockIndexedManager, mockIndexedOperator)

			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.SetIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Item: &grpc_ops.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
			}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			// build stubs
			mockIndexedManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockIndexedOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockIndexedOperator)

			item := indexed_operation_models.GetItemFromRequest(payload.GetItem())
			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				UpdateIndexed(ctx, gomock.Eq(item), gomock.Eq(index)).
				Times(1).
				Return(fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedSet(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
		},
	)

	t.Run(
		"success value retrieved",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockIndexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			mockIndexedOperator := mock_indexed_operator.NewMockIndexedOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			indexedOperator := NewBadgerDBIndexerOperatorServiceController(
				logger, mockBinder, mockIndexedManager, mockIndexedOperator)

			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.SetIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Item: &grpc_ops.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
			}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			// build stubs
			mockIndexedManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockIndexedOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockIndexedOperator)

			item := indexed_operation_models.GetItemFromRequest(payload.GetItem())
			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				UpdateIndexed(ctx, gomock.Eq(item), gomock.Eq(index)).
				Times(1).
				Return(nil)

			resp, err := indexedOperator.IndexedSet(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
		},
	)
}
