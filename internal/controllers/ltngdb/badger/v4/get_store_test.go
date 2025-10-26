package badgerdb_controller_v4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/mocks"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func TestBadgerDBManagerServiceController_GetStore(t *testing.T) {
	t.Run(
		"fails to get a store - internal",
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

			dataName := "any-string"
			dbInfo := &badgerdb_management_models_v4.DBInfo{}
			manager.
				EXPECT().
				GetDBInfo(ctx, gomock.Eq(dataName)).
				Times(1).
				Return(dbInfo, fmt.Errorf("any-error"))
			payload := &grpc_ltngdb.GetStoreRequest{
				Name: dataName,
			}
			resp, err := service.GetStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"successfully gets a store",
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

			dataName := "any-string"
			dbInfo := &badgerdb_management_models_v4.DBInfo{
				Name:         dataName,
				Path:         "any-string",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			manager.
				EXPECT().
				GetDBInfo(ctx, gomock.Eq(dbInfo.Name)).
				Times(1).
				Return(dbInfo, nil)
			payload := &grpc_ltngdb.GetStoreRequest{
				Name: dataName,
			}
			resp, err := service.GetStore(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, resp.DbInfo.Name, dbInfo.Name)
			require.Equal(t, resp.DbInfo.Path, dbInfo.Path)
			require.Equal(t, resp.DbInfo.CreatedAt, timestamppb.New(dbInfo.CreatedAt))
			require.Equal(t, resp.DbInfo.LastOpenedAt, timestamppb.New(dbInfo.LastOpenedAt))
			t.Log(resp)
		},
	)
}
