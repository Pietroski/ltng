package badgerdb_controller_v4

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/mocks"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_query_config "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/queries/config"
)

func Test_CheckLightingNodeEngine(t *testing.T) {
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
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockedBinder),
				WithManger(manager),
			)
			require.NoError(t, err)

			params := &grpc_query_config.CheckEngineRequest{
				Engine: common_model.BadgerDBV4EngineVersionType.String(),
			}
			_, err = service.CheckLightingNodeEngine(ctx, params)
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
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(mockedBinder),
				WithManger(manager),
			)
			require.NoError(t, err)

			params := &grpc_query_config.CheckEngineRequest{
				Engine: "any-engine",
			}
			_, err = service.CheckLightingNodeEngine(ctx, params)
			assert.Error(t, err)
		},
	)
}
