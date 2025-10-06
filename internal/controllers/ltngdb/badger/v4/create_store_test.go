package badgerdb_controller_v4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	mock_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder/mocks"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/mocks"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func TestBadgerDBManagerServiceController_CreateStore(t *testing.T) {
	t.Run(
		"fails to create a store - invalid",
		func(t *testing.T) {
			ctx := context.Background()
			logger := slogx.New()

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

			payload := &grpc_ltngdb.CreateStoreRequest{
				Name: "any-string",
				Path: "any-string",
			}
			var r badgerdb_management_models_v4.CreateStoreRequest
			mockedBinder.
				EXPECT().
				ShouldBind(gomock.Eq(payload), gomock.Eq(&r)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			resp, err := service.CreateStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"fails to create a store - internal",
		func(t *testing.T) {
			ctx := context.Background()
			logger := slogx.New()
			s := serializer.NewJsonSerializer()
			validator := go_validator.NewStructValidator()
			binder := go_binder.NewStructBinder(s, validator)

			ctrl := gomock.NewController(t)
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(binder),
				WithManger(manager),
			)
			require.NoError(t, err)

			data := &badgerdb_management_models_v4.DBInfo{
				Name:         "any-string",
				Path:         "any-string",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			manager.
				EXPECT().
				CreateStore(gomock.Eq(ctx), EqDBInfo(data)).
				Times(1).
				Return(fmt.Errorf("any-error"))
			payload := &grpc_ltngdb.CreateStoreRequest{
				Name: data.Name,
				Path: data.Path,
			}
			resp, err := service.CreateStore(ctx, payload)
			require.Error(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)

	t.Run(
		"successfully creates a store",
		func(t *testing.T) {
			ctx := context.Background()
			logger := slogx.New()
			s := serializer.NewJsonSerializer()
			validator := go_validator.NewStructValidator()
			binder := go_binder.NewStructBinder(s, validator)

			ctrl := gomock.NewController(t)
			manager := mocks.NewMockManager(ctrl)
			service, err := New(ctx,
				WithConfig(config),
				WithLogger(logger),
				WithBinder(binder),
				WithManger(manager),
			)
			require.NoError(t, err)

			data := &badgerdb_management_models_v4.DBInfo{
				Name:         "any-string",
				Path:         "any-string",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			manager.
				EXPECT().
				CreateStore(ctx, EqDBInfo(data)).
				Times(1).
				Return(nil)
			payload := &grpc_ltngdb.CreateStoreRequest{
				Name: data.Name,
				Path: data.Path,
			}
			resp, err := service.CreateStore(ctx, payload)
			require.NoError(t, err)
			require.NotNil(t, resp)
			t.Log(resp)
		},
	)
}
