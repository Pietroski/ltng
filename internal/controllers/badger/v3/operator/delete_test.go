package badgerdb_operator_controller_v3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mock_go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	mock_badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager/mocks"
	mock_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations/mocks"
	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"
	badgerdb_operation_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/operation"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func TestBadgerDBServiceController_Delete(t *testing.T) {
	t.Run(
		"error getting db info from memory or disk",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockManager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			mockOperator := mock_operator.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			controllerParams := &BadgerDBOperatorServiceControllerV3Params{
				Config:   config,
				Logger:   logger,
				Binder:   mockBinder,
				Manager:  mockManager,
				Operator: mockOperator,
			}
			operator, err := NewBadgerDBOperatorServiceControllerV3(controllerParams)
			require.NoError(t, err)

			dbName := "operator-database-unit-test"
			payload := &grpc_ops.DeleteRequest{
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
				Item: &grpc_ops.Item{
					Key: []byte("test-key"),
				},
			}

			// build stubs
			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(&badgerdb_management_models_v3.DBMemoryInfo{}, fmt.Errorf("any-error"))

			resp, err := operator.Delete(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
		},
	)

	t.Run(
		"error operating on giving database",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockManager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			mockOperator := mock_operator.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			controllerParams := &BadgerDBOperatorServiceControllerV3Params{
				Config:   config,
				Logger:   logger,
				Binder:   mockBinder,
				Manager:  mockManager,
				Operator: mockOperator,
			}
			operator, err := NewBadgerDBOperatorServiceControllerV3(controllerParams)
			require.NoError(t, err)

			dbName := "operator-database-unit-test"
			payload := &grpc_ops.DeleteRequest{
				Item: &grpc_ops.Item{Key: []byte("test-key")},
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			dbInfo := &badgerdb_management_models_v3.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v3.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqItem := payload.GetItem()
			item := &badgerdb_operation_models_v3.Item{
				Key: reqItem.GetKey(),
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v3.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v3.IndexProperties{
					IndexDeletionBehaviour: badgerdb_operation_models_v3.IndexDeletionBehaviour(
						reqOpts.GetIndexingProperties().GetIndexDeletionBehaviour(),
					),
				},
			}

			reqRetrialOpts := payload.GetRetrialOpts()
			retrialOpts := &chainded_operator.RetrialOpts{
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
				Delete(ctx, item, opts, retrialOpts).
				Times(1).
				Return(fmt.Errorf("any-error"))

			resp, err := operator.Delete(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
		},
	)

	t.Run(
		"success value deleted",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockManager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			mockOperator := mock_operator.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			controllerParams := &BadgerDBOperatorServiceControllerV3Params{
				Config:   config,
				Logger:   logger,
				Binder:   mockBinder,
				Manager:  mockManager,
				Operator: mockOperator,
			}
			operator, err := NewBadgerDBOperatorServiceControllerV3(controllerParams)
			require.NoError(t, err)

			dbName := "operator-database-unit-test"
			payload := &grpc_ops.DeleteRequest{
				Item: &grpc_ops.Item{Key: []byte("test-key")},
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			dbInfo := &badgerdb_management_models_v3.DBMemoryInfo{
				Name:         dbName,
				Path:         badgerdb_manager_adaptor_v3.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqItem := payload.GetItem()
			item := &badgerdb_operation_models_v3.Item{
				Key: reqItem.GetKey(),
			}

			reqOpts := payload.GetIndexOpts()
			opts := &badgerdb_operation_models_v3.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
				IndexProperties: badgerdb_operation_models_v3.IndexProperties{
					IndexDeletionBehaviour: badgerdb_operation_models_v3.IndexDeletionBehaviour(
						reqOpts.GetIndexingProperties().GetIndexDeletionBehaviour(),
					),
				},
			}

			reqRetrialOpts := payload.GetRetrialOpts()
			retrialOpts := &chainded_operator.RetrialOpts{
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
				Delete(ctx, item, opts, retrialOpts).
				Times(1).
				Return(nil)

			resp, err := operator.Delete(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
		},
	)
}
