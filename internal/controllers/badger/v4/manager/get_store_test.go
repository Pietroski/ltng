package badgerdb_manager_controller_v4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	mock_badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager/mocks"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func TestBadgerDBManagerServiceController_GetStore(t *testing.T) {
	t.Run(
		"fails to get a store - invalid",
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

			dataName := "any-string"
			payload := &grpc_mngmt.GetStoreRequest{
				Name: dataName,
			}
			var r badgerdb_management_models_v4.GetStoreRequest
			mockedBinder.
				EXPECT().
				ShouldBind(gomock.Eq(payload), gomock.Eq(&r)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			resp, err := service.GetStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"fails to get a store - internal",
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

			dataName := "any-string"
			dbInfo := &badgerdb_management_models_v4.DBInfo{}
			manager.
				EXPECT().
				GetDBInfo(ctx, gomock.Eq(dataName)).
				Times(1).
				Return(dbInfo, fmt.Errorf("any-error"))
			payload := &grpc_mngmt.GetStoreRequest{
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
			payload := &grpc_mngmt.GetStoreRequest{
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
