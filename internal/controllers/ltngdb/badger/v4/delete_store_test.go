package badgerdb_controller_v4

import (
	"context"
	"fmt"
	"testing"

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

func TestBadgerDBManagerServiceController_DeleteStore(t *testing.T) {
	t.Run(
		"fails to delete a store - invalid",
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

			dbName := "any-string"
			payload := &grpc_ltngdb.DeleteStoreRequest{
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
