package badgerdb_indexed_manager_controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	mock_indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed/mocks"
	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"
)

func TestBadgerDBManagerServiceController_CreateStore(t *testing.T) {
	t.Run(
		"fails to create a store - invalid",
		func(t *testing.T) {
			ctx := context.Background()
			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)
			//serializer := go_serializer.NewJsonSerializer()
			//validator := go_validator.NewStructValidator()

			ctrl := gomock.NewController(t)
			binder := mock_binder.NewMockBinder(ctrl)
			//manager := mock_manager.NewMockManager(ctrl)
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			payload := &grpc_indexed_mngmnt.CreateIndexedStoreRequest{
				Name: "any-string",
				Path: "any-string",
			}
			var r management_models.CreateStoreRequest
			binder.
				EXPECT().
				ShouldBind(gomock.Eq(payload), gomock.Eq(&r)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			resp, err := service.CreateIndexedStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"fails to create a store - internal",
		func(t *testing.T) {
			ctx := context.Background()
			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)
			serializer := go_serializer.NewJsonSerializer()
			validator := go_validator.NewStructValidator()
			binder := go_binder.NewStructBinder(serializer, validator)

			ctrl := gomock.NewController(t)
			//manager := mock_manager.NewMockManager(ctrl)
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			data := &management_models.DBInfo{
				Name:         "any-string",
				Path:         "any-string",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			indexedManager.
				EXPECT().
				CreateIndexedStore(ctx, EqDBInfo(data)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			payload := &grpc_indexed_mngmnt.CreateIndexedStoreRequest{
				Name: data.Name,
				Path: data.Path,
			}
			resp, err := service.CreateIndexedStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"successfully creates a store",
		func(t *testing.T) {
			ctx := context.Background()
			loggerPublishers := &go_logger.Publishers{}
			loggerOpts := &go_logger.Opts{
				Debug:   true,
				Publish: true,
			}
			logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)
			serializer := go_serializer.NewJsonSerializer()
			validator := go_validator.NewStructValidator()
			binder := go_binder.NewStructBinder(serializer, validator)

			ctrl := gomock.NewController(t)
			//manager := mock_manager.NewMockManager(ctrl)
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			data := &management_models.DBInfo{
				Name:         "any-string",
				Path:         "any-string",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			indexedManager.
				EXPECT().
				CreateIndexedStore(ctx, EqDBInfo(data)).
				Times(1).
				Return(nil)
			payload := &grpc_indexed_mngmnt.CreateIndexedStoreRequest{
				Name: data.Name,
				Path: data.Path,
			}
			resp, err := service.CreateIndexedStore(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)
}
