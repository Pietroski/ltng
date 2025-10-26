package badgerdb_controller_v4

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/mocks"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func TestBadgerDBManagerServiceController_DeleteStore(t *testing.T) {
	t.Run(
		"fails to delete a store - internal",
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

			dbName := "any-string"
			manager.
				EXPECT().
				DeleteStore(ctx, dbName).
				Times(1).
				Return(fmt.Errorf("any-error"))
			payload := &grpc_ltngdb.DeleteStoreRequest{
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
			logger := slogx.New()

			ctrl := gomock.NewController(t)
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithManger(manager),
			)
			require.NoError(t, err)

			dbName := "any-string"
			manager.
				EXPECT().
				DeleteStore(ctx, dbName).
				Times(1).
				Return(nil)
			payload := &grpc_ltngdb.DeleteStoreRequest{
				Name: dbName,
			}
			resp, err := service.DeleteStore(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)
}
