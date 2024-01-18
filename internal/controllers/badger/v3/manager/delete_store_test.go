package badgerdb_manager_controller_v3

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager/mocks"
	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func TestBadgerDBManagerServiceController_DeleteStore(t *testing.T) {
	t.Run(
		"fails to delete a store - invalid",
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
			mockedBinder := mock_binder.NewMockBinder(ctrl)
			manager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			controllerParams := &BadgerDBManagerServiceControllerV3Params{
				Config:  config,
				Logger:  logger,
				Binder:  mockedBinder,
				Manager: manager,
			}
			service, err := NewBadgerDBManagerServiceControllerV3(controllerParams)
			require.NoError(t, err)

			dbName := "any-string"
			payload := &grpc_mngmt.DeleteStoreRequest{
				Name: dbName,
			}
			var r badgerdb_management_models_v3.DeleteStoreRequest
			mockedBinder.
				EXPECT().
				ShouldBind(gomock.Eq(payload), gomock.Eq(&r)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			resp, err := service.DeleteStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"fails to delete a store - internal",
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
			manager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			controllerParams := &BadgerDBManagerServiceControllerV3Params{
				Config:  config,
				Logger:  logger,
				Binder:  binder,
				Manager: manager,
			}
			service, err := NewBadgerDBManagerServiceControllerV3(controllerParams)
			require.NoError(t, err)

			dbName := "any-string"
			manager.
				EXPECT().
				DeleteStore(ctx, dbName).
				Times(1).
				Return(fmt.Errorf("any-error"))
			payload := &grpc_mngmt.DeleteStoreRequest{
				Name: dbName,
			}
			resp, err := service.DeleteStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"successfully deletes a store",
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
			manager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			controllerParams := &BadgerDBManagerServiceControllerV3Params{
				Config:  config,
				Logger:  logger,
				Binder:  binder,
				Manager: manager,
			}
			service, err := NewBadgerDBManagerServiceControllerV3(controllerParams)
			require.NoError(t, err)

			dbName := "any-string"
			manager.
				EXPECT().
				DeleteStore(ctx, dbName).
				Times(1).
				Return(nil)
			payload := &grpc_mngmt.DeleteStoreRequest{
				Name: dbName,
			}
			resp, err := service.DeleteStore(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)
}
