package badgerdb_manager_controller_v3

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	mock_badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager/mocks"
	common_model "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/common"
	grpc_query_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/queries/config"
)

func Test_Unit_CheckManagerEngine(t *testing.T) {
	t.Run(
		"happy-path",
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
			manager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			controllerParams := &BadgerDBManagerServiceControllerV3Params{
				Config:  config,
				Logger:  logger,
				Binder:  mockedBinder,
				Manager: manager,
			}
			service, err := NewBadgerDBManagerServiceControllerV3(controllerParams)
			require.NoError(t, err)

			params := &grpc_query_config.CheckEngineRequest{
				Engine: common_model.BadgerDBV3EngineVersionType.String(),
			}
			_, err = service.CheckManagerEngine(ctx, params)
			assert.NoError(t, err)
		},
	)

	t.Run(
		"invalid version",
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
			manager := mock_badgerdb_manager_adaptor_v3.NewMockManager(ctrl)
			controllerParams := &BadgerDBManagerServiceControllerV3Params{
				Config:  config,
				Logger:  logger,
				Binder:  mockedBinder,
				Manager: manager,
			}
			service, err := NewBadgerDBManagerServiceControllerV3(controllerParams)
			require.NoError(t, err)

			params := &grpc_query_config.CheckEngineRequest{
				Engine: "any-engine",
			}
			_, err = service.CheckManagerEngine(ctx, params)
			assert.Error(t, err)
		},
	)
}
