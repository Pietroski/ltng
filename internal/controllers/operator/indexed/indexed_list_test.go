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
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/search"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	mock_go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

func TestBadgerDBServiceController_List(t *testing.T) {
	t.Run(
		"error binding and/or validating payload data",
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

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.Items)
		},
	)

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

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest
			dbInfo := &management_models.DBMemoryInfo{}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockIndexedManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.Items)
		},
	)

	t.Run(
		"invalid pagination",
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

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest
			//dbPayload := &management_models.PaginationRequest{}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

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

			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				ListIndexed(ctx, index, &pagination).
				Times(1).
				Return(operation_models.OpList{}, fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.Items)
		},
	)

	t.Run(
		"error operating on giving database - all",
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

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest
			//dbPayload := &management_models.PaginationRequest{}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

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

			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				ListIndexed(ctx, index, &pagination).
				Times(1).
				Return(operation_models.OpList{}, fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.Items)
		},
	)

	t.Run(
		"error operating on giving database - paginated",
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

			page := 2
			size := 2
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

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

			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				ListIndexed(ctx, index, &pagination).
				Times(1).
				Return(operation_models.OpList{}, fmt.Errorf("any-error"))

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.Items)
		},
	)

	t.Run(
		"success list retrieved - all",
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

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest
			//dbPayload := &management_models.PaginationRequest{}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}
			listValues := operation_models.OpList{
				{
					Key:   []byte("key-1"),
					Value: []byte("value-1"),
				},
				{
					Key:   []byte("key-2"),
					Value: []byte("value-2"),
				},
				{
					Key:   []byte("key-3"),
					Value: []byte("value-3"),
				},
				{
					Key:   []byte("key-4"),
					Value: []byte("value-4"),
				},
				{
					Key:   []byte("key-5"),
					Value: []byte("value-5"),
				},
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

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

			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				ListIndexed(ctx, index, &pagination).
				Times(1).
				Return(listValues, nil)

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)

			for idx, item := range resp.Items {
				t.Log("item ->", item)
				require.Equal(t, item.Key, listValues[idx].Key)
				require.Equal(t, item.Value, listValues[idx].Value)
			}
		},
	)

	t.Run(
		"success list retrieved - paginated",
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

			page := 2
			size := 2
			dbName := "operator-database-unit-test"
			payload := &grpc_indexed_ops.ListIndexedRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Index: &grpc_indexed_ops.Index{
					ShallIndex:      false,
					ParentKey:       nil,
					IndexKeys:       nil,
					IndexProperties: nil,
				},
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var pagination management_models.PaginationRequest
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}
			listValues := operation_models.OpList{
				{
					Key:   []byte("key-1"),
					Value: []byte("value-1"),
				},
				{
					Key:   []byte("key-2"),
					Value: []byte("value-2"),
				},
				{
					Key:   []byte("key-3"),
					Value: []byte("value-3"),
				},
				{
					Key:   []byte("key-4"),
					Value: []byte("value-4"),
				},
				{
					Key:   []byte("key-5"),
					Value: []byte("value-5"),
				},
			}
			returnedList := listValues[2:4]

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

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

			index := indexed_operation_models.GetIndexFromRequest(payload.GetIndex())
			mockIndexedOperator.
				EXPECT().
				ListIndexed(ctx, index, &pagination).
				Times(1).
				Return(listValues[2:4], nil)

			resp, err := indexedOperator.IndexedList(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)

			for idx, item := range resp.Items {
				require.Equal(t, item.Key, returnedList[idx].Key)
				require.Equal(t, item.Value, returnedList[idx].Value)
			}
		},
	)
}
