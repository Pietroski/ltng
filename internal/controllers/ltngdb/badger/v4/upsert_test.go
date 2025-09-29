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
	list_operator "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func TestBadgerDBServiceController_Upsert(t *testing.T) {
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

			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.UpsertRequest{
				Item: &grpc_ltngdb.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}

			// build stubs
			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(&badgerdb_management_models_v4.DBMemoryInfo{}, fmt.Errorf("any-error"))

			resp, err := operator.Upsert(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
		},
	)

	t.Run(
		"error operating on giving database",
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

			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.UpsertRequest{
				Item: &grpc_ltngdb.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqItem := payload.GetItem()
			item := &badgerdb_operation_models_v4.Item{
				Key:   reqItem.GetKey(),
				Value: reqItem.GetValue(),
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
			}

			reqRetrialOpts := payload.GetRetrialOpts()
			retrialOpts := &list_operator.RetrialOpts{
				RetrialOnErr: reqRetrialOpts.GetRetrialOnError(),
				RetrialCount: int(reqRetrialOpts.GetRetrialCount()),
			}

			// build stubs
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
				Upsert(ctx, item, opts, retrialOpts).
				Times(1).
				Return(fmt.Errorf("any-error"))

			resp, err := operator.Upsert(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
		},
	)

	t.Run(
		"success value retrieved",
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

			dbName := "operator-database-unit-test"
			payload := &grpc_ltngdb.UpsertRequest{
				Item: &grpc_ltngdb.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			dbInfo := &badgerdb_management_models_v4.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v4.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqItem := payload.GetItem()
			item := &badgerdb_operation_models_v4.Item{
				Key:   reqItem.GetKey(),
				Value: reqItem.GetValue(),
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v4.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
			}

			reqRetrialOpts := payload.GetRetrialOpts()
			retrialOpts := &list_operator.RetrialOpts{
				RetrialOnErr: reqRetrialOpts.GetRetrialOnError(),
				RetrialCount: int(reqRetrialOpts.GetRetrialCount()),
			}

			// build stubs
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
				Upsert(ctx, item, opts, retrialOpts).
				Times(1).
				Return(nil)

			resp, err := operator.Upsert(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
		},
	)
}
