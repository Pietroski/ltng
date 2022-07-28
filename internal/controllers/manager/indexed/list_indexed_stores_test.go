package badgerdb_indexed_manager_controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	mock_indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed/mocks"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/search"
	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"
)

func TestBadgerDBManagerServiceController_ListStores(t *testing.T) {
	t.Run(
		"fails to list stores - no pagination - invalid",
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
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, mockedBinder, indexedManager)

			payload := &grpc_indexed_mngmnt.ListIndexedStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   0,
					PageSize: 0,
				},
			}
			var r management_models.PaginationRequest
			mockedBinder.
				EXPECT().
				ShouldBind(gomock.Eq(payload.Pagination), gomock.Eq(&r)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			respList, err := service.ListIndexedStores(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, respList)
			require.Nil(t, respList.DbsInfos)
			t.Log(respList)
		},
	)

	t.Run(
		"fails to list stores - pagination - invalid",
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
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, mockedBinder, indexedManager)

			size, page := 2, 2
			payload := &grpc_indexed_mngmnt.ListIndexedStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			var r management_models.PaginationRequest
			mockedBinder.
				EXPECT().
				ShouldBind(gomock.Eq(payload.Pagination), gomock.Eq(&r)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			respList, err := service.ListIndexedStores(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, respList)
			require.Nil(t, respList.DbsInfos)
			t.Log(respList)
		},
	)

	t.Run(
		"fails to list stores - no pagination - internal",
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
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			var dbsInfos []*management_models.DBInfo
			indexedManager.
				EXPECT().
				ListIndexedStoreInfo(ctx, gomock.Eq(0), gomock.Eq(0)).
				Times(1).
				Return(dbsInfos, fmt.Errorf("any-error"))
			payload := &grpc_indexed_mngmnt.ListIndexedStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   0,
					PageSize: 0,
				},
			}
			respList, err := service.ListIndexedStores(ctx, payload)
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
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			size, page := 2, 2
			var dbsInfos []*management_models.DBInfo
			indexedManager.
				EXPECT().
				ListIndexedStoreInfo(ctx, gomock.Eq(size), gomock.Eq(page)).
				Times(1).
				Return(dbsInfos, fmt.Errorf("any-error"))
			payload := &grpc_indexed_mngmnt.ListIndexedStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			respList, err := service.ListIndexedStores(ctx, payload)
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
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			dbsInfos := []*management_models.DBInfo{
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
			indexedManager.
				EXPECT().
				ListIndexedStoreInfo(ctx, gomock.Eq(0), gomock.Eq(0)).
				Times(1).
				Return(dbsInfos, nil)
			payload := &grpc_indexed_mngmnt.ListIndexedStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   0,
					PageSize: 0,
				},
			}
			respList, err := service.ListIndexedStores(ctx, payload)
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
			indexedManager := mock_indexed_manager.NewMockIndexerManager(ctrl)
			service := NewBadgerDBIndexerManagerServiceController(logger, binder, indexedManager)

			size, page := 2, 2
			pageOffset := 2
			dbsInfos := []*management_models.DBInfo{
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
			indexedManager.
				EXPECT().
				ListIndexedStoreInfo(ctx, gomock.Eq(size), gomock.Eq(page)).
				Times(1).
				Return(dbsInfos[pageOffset:], nil)
			payload := &grpc_indexed_mngmnt.ListIndexedStoresRequest{
				Pagination: &grpc_pagination.Pagination{
					PageId:   uint32(page),
					PageSize: uint32(size),
				},
			}
			respList, err := service.ListIndexedStores(ctx, payload)
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
