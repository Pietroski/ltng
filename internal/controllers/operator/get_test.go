package badgerdb_operator_controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/adaptors/datastore/badgerdb/manager"
	mock_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/adaptors/datastore/badgerdb/manager/mocks"
	mock_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations/mocks"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/models/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/schemas/generated/go/transactions/operations"
	mock_go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

func TestBadgerDBServiceController_Get(t *testing.T) {
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
			payload := &grpc_ops.GetRequest{
				Key: []byte("test-key"),
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

			resp, err := operator.Get(ctx, payload)
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
			payload := &grpc_ops.GetRequest{
				Key: []byte("test-key"),
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
				Load(payload.Key).
				Times(1).
				Return([]byte{}, fmt.Errorf("any-error"))

			resp, err := operator.Get(ctx, payload)
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
			payload := &grpc_ops.GetRequest{
				Key: []byte("test-key"),
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
			keyValue := []byte("test-key-value")

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
				Load(payload.Key).
				Times(1).
				Return(keyValue, nil)

			resp, err := operator.Get(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)

			require.Equal(t, resp.Value, keyValue)
		},
	)
}
