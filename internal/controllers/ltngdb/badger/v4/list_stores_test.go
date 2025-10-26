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
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func TestBadgerDBManagerServiceController_ListStores(t *testing.T) {
	t.Run(
		"fails to list stores - no pagination - internal",
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

			var dbsInfos []*badgerdb_management_models_v4.DBInfo
			manager.
				EXPECT().
				ListStoreInfo(ctx, gomock.Eq(0), gomock.Eq(0)).
				Times(1).
				Return(dbsInfos, fmt.Errorf("any-error"))
			payload := &grpc_ltngdb.ListStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   0,
					PageSize: 0,
				},
			}
			respList, err := service.ListStores(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, respList)
			require.Nil(t, respList.DbsInfos)
			t.Log(respList)
		},
	)

	t.Run(
		"fails to list stores - pagination - internal",
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

			size, page := 2, 2
			var dbsInfos []*badgerdb_management_models_v4.DBInfo
			manager.
				EXPECT().
				ListStoreInfo(ctx, gomock.Eq(size), gomock.Eq(page)).
				Times(1).
				Return(dbsInfos, fmt.Errorf("any-error"))
			payload := &grpc_ltngdb.ListStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
			}
			respList, err := service.ListStores(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, respList)
			require.Nil(t, respList.DbsInfos)
			t.Log(respList)
		},
	)

	t.Run(
		"successfully lists stores - no pagination",
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

			dbsInfos := []*badgerdb_management_models_v4.DBInfo{
				{
					Name:         "any-string-1",
					Path:         "any-string-1",
					CreatedAt:    time.Now(),
					LastOpenedAt: time.Now(),
				},
				{
					Name:         "any-string-2",
					Path:         "any-string-2",
					CreatedAt:    time.Now().Add(time.Second),
					LastOpenedAt: time.Now().Add(time.Second),
				},
				{
					Name:         "any-string-3",
					Path:         "any-string-3",
					CreatedAt:    time.Now().Add(time.Second * 2),
					LastOpenedAt: time.Now().Add(time.Second * 2),
				},
				{
					Name:         "any-string-4",
					Path:         "any-string-4",
					CreatedAt:    time.Now().Add(time.Second * 3),
					LastOpenedAt: time.Now().Add(time.Second * 3),
				},
			}
			manager.
				EXPECT().
				ListStoreInfo(ctx, gomock.Eq(0), gomock.Eq(0)).
				Times(1).
				Return(dbsInfos, nil)
			payload := &grpc_ltngdb.ListStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   0,
					PageSize: 0,
				},
			}
			respList, err := service.ListStores(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, respList)
			require.NotNil(t, respList.DbsInfos)
			for idx, resp := range respList.DbsInfos {
				require.Equal(t, resp.Name, dbsInfos[idx].Name)
				require.Equal(t, resp.Path, dbsInfos[idx].Path)
				require.Equal(t, resp.CreatedAt, timestamppb.New(dbsInfos[idx].CreatedAt))
				require.Equal(t, resp.LastOpenedAt, timestamppb.New(dbsInfos[idx].LastOpenedAt))
			}
			t.Log(respList)
		},
	)

	t.Run(
		"successfully lists stores - pagination",
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

			size, page := 2, 2
			pageOffset := 2
			dbsInfos := []*badgerdb_management_models_v4.DBInfo{
				{
					Name:         "any-string-1",
					Path:         "any-string-1",
					CreatedAt:    time.Now(),
					LastOpenedAt: time.Now(),
				},
				{
					Name:         "any-string-2",
					Path:         "any-string-2",
					CreatedAt:    time.Now().Add(time.Second),
					LastOpenedAt: time.Now().Add(time.Second),
				},
				{
					Name:         "any-string-3",
					Path:         "any-string-3",
					CreatedAt:    time.Now().Add(time.Second * 2),
					LastOpenedAt: time.Now().Add(time.Second * 2),
				},
				{
					Name:         "any-string-4",
					Path:         "any-string-4",
					CreatedAt:    time.Now().Add(time.Second * 3),
					LastOpenedAt: time.Now().Add(time.Second * 3),
				},
			}
			manager.
				EXPECT().
				ListStoreInfo(ctx, gomock.Eq(size), gomock.Eq(page)).
				Times(1).
				Return(dbsInfos[pageOffset:], nil)
			payload := &grpc_ltngdb.ListStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint64(page),
					PageSize: uint64(size),
				},
			}
			respList, err := service.ListStores(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, respList)
			require.NotNil(t, respList.DbsInfos)
			for idx, resp := range respList.DbsInfos {
				require.Equal(t, resp.Name, dbsInfos[idx+pageOffset].Name)
				require.Equal(t, resp.Path, dbsInfos[idx+pageOffset].Path)
				require.Equal(t, resp.CreatedAt, timestamppb.New(dbsInfos[idx+pageOffset].CreatedAt))
				require.Equal(t, resp.LastOpenedAt, timestamppb.New(dbsInfos[idx+pageOffset].LastOpenedAt))
			}
			t.Log(respList)
		},
	)
}
