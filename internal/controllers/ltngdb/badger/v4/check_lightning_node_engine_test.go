package badgerdb_controller_v4

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/mocks"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_query_config "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/queries/config"
)

func Test_CheckLightingNodeEngine(t *testing.T) {
	t.Run(
		"happy-path",
		func(t *testing.T) {
			ctx := context.Background()
			logger := slogx.New()

			ctrl := gomock.NewController(t)
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
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
			logger := slogx.New()

			ctrl := gomock.NewController(t)
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
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
