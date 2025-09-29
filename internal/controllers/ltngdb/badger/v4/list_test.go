package badgerdb_controller_v4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mock_go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"
	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/mocks"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func TestBadgerDBServiceController_List(t *testing.T) {
	t.Run(
		"error binding and/or validating payload data",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(fmt.Errorf("any-error"))

			resp, err := operator.List(ctx, payload)
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
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, fmt.Errorf("any-error"))

			resp, err := operator.List(ctx, payload)
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
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination
			//dbPayload := &badgerdb_management_models_v4.Pagination{}
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
					ListSearchPattern: badgerdb_operation_models_v4.ListSearchPattern(
						reqOpts.GetIndexingProperties().GetListSearchPattern(),
					),
				},
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockOperator)

			mockOperator.
				EXPECT().
				List(ctx, opts, &pagination).
				Times(1).
				Return(badgerdb_operation_models_v4.Items{}, fmt.Errorf("any-error"))

			resp, err := operator.List(ctx, payload)
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
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination
			//dbPayload := &badgerdb_management_models_v4.Pagination{}
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
					ListSearchPattern: badgerdb_operation_models_v4.ListSearchPattern(
						reqOpts.GetIndexingProperties().GetListSearchPattern(),
					),
				},
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockOperator)

			mockOperator.
				EXPECT().
				List(ctx, opts, &pagination).
				Times(1).
				Return(badgerdb_operation_models_v4.Items{}, fmt.Errorf("any-error"))

			resp, err := operator.List(ctx, payload)
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
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 2
			size := 2
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
					ListSearchPattern: badgerdb_operation_models_v4.ListSearchPattern(
						reqOpts.GetIndexingProperties().GetListSearchPattern(),
					),
				},
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockOperator)

			mockOperator.
				EXPECT().
				List(ctx, opts, &pagination).
				Times(1).
				Return(badgerdb_operation_models_v4.Items{}, fmt.Errorf("any-error"))

			resp, err := operator.List(ctx, payload)
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
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 0
			size := 0
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination
			//dbPayload := &badgerdb_management_models_v4.Pagination{}
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}
			listValues := badgerdb_operation_models_v4.Items{
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

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
					ListSearchPattern: badgerdb_operation_models_v4.ListSearchPattern(
						reqOpts.GetIndexingProperties().GetListSearchPattern(),
					),
				},
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockOperator)

			mockOperator.
				EXPECT().
				List(ctx, opts, &pagination).
				Times(1).
				Return(listValues, nil)

			resp, err := operator.List(ctx, payload)
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
			mockManager := mocks.NewMockManager(ctrl)
			mockOperator := mocks.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockBinder),
				WithManger(mockManager),
				WithOperator(mockOperator),
			)
			require.NoError(t, err)

			page := 2
			size := 2
			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.ListRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			var pagination badgerdb_management_models_v4.Pagination
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}
			listValues := badgerdb_operation_models_v4.Items{
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

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
					ListSearchPattern: badgerdb_operation_models_v4.ListSearchPattern(
						reqOpts.GetIndexingProperties().GetListSearchPattern(),
					),
				},
			}

			// build stubs
			mockBinder.
				EXPECT().
				ShouldBind(payload.Pagination, &pagination).
				Times(1).
				Return(nil)

			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(dbInfo, nil)

			mockOperator.
				EXPECT().
				Operate(dbInfo).
				Times(1).
				Return(mockOperator)

			mockOperator.
				EXPECT().
				List(ctx, opts, &pagination).
				Times(1).
				Return(listValues[2:4], nil)

			resp, err := operator.List(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)

			for idx, item := range resp.Items {
				require.Equal(t, item.Key, returnedList[idx].Key)
				require.Equal(t, item.Value, returnedList[idx].Value)
			}
		},
	)
}
