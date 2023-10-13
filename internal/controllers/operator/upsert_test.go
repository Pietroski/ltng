package badgerdb_operator_controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mock_go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager/mocks"
	mock_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations/mocks"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func TestBadgerDBServiceController_Upsert(t *testing.T) {
	t.Run(
		"error getting db info from memory or disk",
		func(t *testing.T) {
			ctx := context.Background()

			ctrl := gomock.NewController(t)
			mockManager := mock_manager.NewMockManager(ctrl)
			mockOperator := mock_operator.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator := NewBadgerDBOperatorServiceController(logger, mockBinder, mockManager, mockOperator)

			dbName := "operator-database-unit-test"
			payload := &grpc_ops.UpsertRequest{
				Item: &grpc_ops.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}

			// build stubs
			mockManager.
				EXPECT().
				GetDBMemoryInfo(ctx, payload.DatabaseMetaInfo.DatabaseName).
				Times(1).
				Return(&management_models.DBMemoryInfo{}, fmt.Errorf("any-error"))

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
			mockManager := mock_manager.NewMockManager(ctrl)
			mockOperator := mock_operator.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator := NewBadgerDBOperatorServiceController(logger, mockBinder, mockManager, mockOperator)

			dbName := "operator-database-unit-test"
			payload := &grpc_ops.UpsertRequest{
				Item: &grpc_ops.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqItem := payload.GetItem()
			item := &operation_models.Item{
				Key:   reqItem.GetKey(),
				Value: reqItem.GetValue(),
			}

			reqOpts := payload.GetIndexOpts()
			opts := &operation_models.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
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
			mockManager := mock_manager.NewMockManager(ctrl)
			mockOperator := mock_operator.NewMockOperator(ctrl)
			mockBinder := mock_go_binder.NewMockBinder(ctrl)

			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

			operator := NewBadgerDBOperatorServiceController(logger, mockBinder, mockManager, mockOperator)

			dbName := "operator-database-unit-test"
			payload := &grpc_ops.UpsertRequest{
				Item: &grpc_ops.Item{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
				DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{
					DatabaseName: dbName,
				},
			}
			dbInfo := &management_models.DBMemoryInfo{
				Name:         dbName,
				Path:         manager.InternalLocalManagement + "/test" + dbName,
				CreatedAt:    time.Time{},
				LastOpenedAt: time.Time{},
				DB:           nil,
			}

			reqItem := payload.GetItem()
			item := &operation_models.Item{
				Key:   reqItem.GetKey(),
				Value: reqItem.GetValue(),
			}

			reqOpts := payload.GetIndexOpts()
			opts := &operation_models.IndexOpts{
				HasIdx:       reqOpts.GetHasIdx(),
				ParentKey:    reqOpts.GetParentKey(),
				IndexingKeys: reqOpts.GetIndexingKeys(),
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
				Upsert(ctx, item, opts, retrialOpts).
				Times(1).
				Return(nil)

			resp, err := operator.Upsert(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
		},
	)
}
