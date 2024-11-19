package badgerdb_manager_controller_v4

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	mock_badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager/mocks"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
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

			ctrl := gomock.NewController(t)
			mockedBinder := mock_binder.NewMockBinder(ctrl)
			manager := mock_badgerdb_manager_adaptor_v4.NewMockManager(ctrl)
			service, err := NewBadgerDBManagerServiceControllerV4(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockedBinder),
				WithManger(manager),
			)
			require.NoError(t, err)

			dbName := "any-string"
			payload := &grpc_mngmt.DeleteStoreRequest{
				Name: dbName,
			}
			var r badgerdb_management_models_v4.DeleteStoreRequest
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
			s := serializer.NewJsonSerializer()
			validator := go_validator.NewStructValidator()
			binder := go_binder.NewStructBinder(s, validator)

			ctrl := gomock.NewController(t)
			manager := mock_badgerdb_manager_adaptor_v4.NewMockManager(ctrl)
			service, err := NewBadgerDBManagerServiceControllerV4(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(binder),
				WithManger(manager),
			)
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
			s := serializer.NewJsonSerializer()
			validator := go_validator.NewStructValidator()
			binder := go_binder.NewStructBinder(s, validator)

			ctrl := gomock.NewController(t)
			manager := mock_badgerdb_manager_adaptor_v4.NewMockManager(ctrl)
			service, err := NewBadgerDBManagerServiceControllerV4(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(binder),
				WithManger(manager),
			)
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
