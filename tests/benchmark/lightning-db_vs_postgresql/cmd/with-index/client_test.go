package cmd

import (
	"context"
	"database/sql"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

gitlab.com/pietroski-software-company/devex/golang/serializer
go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/client"
grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/search"
grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/tests/benchmark/lightning-db_vs_postgresql/config"
sqlc_user_store "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/tests/benchmark/lightning-db_vs_postgresql/internal/adaptors/datastore/postgresql/user/sqlc"
)

const (
	managerServerAddr  = "localhost:50051"
	operatorServerAddr = "localhost:50052"

	clientTestStore     = "client-test-store"
	clientTestStorePath = "client_test_store"
)

type (
	CreateUserParams struct {
		UserID    uuid.UUID `json:"userID"`
		Email     string    `json:"email"`
		Name      string    `json:"name"`
		Surname   string    `json:"surname"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}
)

var (
	payload    CreateUserParams
	ltngClient ltng_client.LTNGClient

	psqlPayload sqlc_user_store.CreateUserParams
	psqlDB      *sql.DB
	userStore   sqlc_user_store.Store

	databaseMetaInfo = &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore}
	serializer       = go_serializer.NewJsonSerializer()

	ltngKey, ltngValue []byte
	ltngEmailIndexKey  []byte
)

func init() {
	ctx := context.Background()

	cfg := &ltng_node_config.Config{}
	err := go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		log.Fatalf("failed to get config: %v", err)
	}

	ltngClient, err = ltng_client.NewLTNGClient(ctx, &ltng_client.LTNGClientParams{
		Addresses: &ltng_client.Addresses{
			Manager:  managerServerAddr,
			Operator: operatorServerAddr,
		},
		Engine: "default",
	})
	if err != nil {
		defer ltngClient.Close()
		log.Fatalf("failed to start ltng client: %v", err)
	}

	psqlDB, err = sql.Open(cfg.PostgreSQL.DriverName, cfg.PostgreSQL.DataSourceName)
	if err != nil {
		defer psqlDB.Close()
		log.Fatalf("failed to start psql client: %v", err)
	}
	userStore = sqlc_user_store.NewUserStore(psqlDB)

	payload = CreateUserParams{
		UserID:    uuid.New(),
		Email:     go_random.RandomEmail(),
		Name:      go_random.RandomString(12),
		Surname:   go_random.RandomString(12),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	psqlPayload = sqlc_user_store.CreateUserParams{
		UserID:  payload.UserID,
		Email:   payload.Email,
		Name:    payload.Name,
		Surname: payload.Surname,
		CreatedAt: sql.NullTime{
			Time:  payload.CreatedAt,
			Valid: true,
		},
		UpdatedAt: sql.NullTime{
			Time:  payload.UpdatedAt,
			Valid: true,
		},
	}

	// Store creation
	{
		_, err = ltngClient.CreateStore(
			ctx,
			&grpc_mngmt.CreateStoreRequest{
				Name: clientTestStore,
				Path: clientTestStorePath,
			},
		)
		if err != nil {
			defer ltngClient.Close()
			log.Fatalf("failed to create ltng store: %v", err)
		}
		//t.Log("created store if it does not exist ->", createdStoreResp)
	}

	ltngKey, err = serializer.Serialize(payload.UserID)
	if err != nil {
		defer ltngClient.Close()
		log.Fatalf("failed to serialize ltng key: %v", err)
	}
	ltngValue, err = serializer.Serialize(payload)
	if err != nil {
		defer ltngClient.Close()
		log.Fatalf("failed to serialize ltng value: %v", err)
	}
	ltngEmailIndexKey, err = serializer.Serialize(payload.Email)
	if err != nil {
		defer ltngClient.Close()
		log.Fatalf("failed to serialize ltng email index key: %v", err)
	}
}

func Test_LightningNode_ServerWithClient(t *testing.T) {
	ctx := context.Background()
	var err error

	begin := time.Now()
	_, err = ltngClient.Load(
		ctx,
		&grpc_ops.LoadRequest{
			DatabaseMetaInfo: databaseMetaInfo,
			Item:             &grpc_ops.Item{Key: ltngKey},
		},
	)
	end := time.Since(begin)
	require.Error(t, err)
	t.Log("get time ->", end.Microseconds())
	//t.Log("Get response string ->", getResp.String())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin2 := time.Now()
	_, err = ltngClient.Create(ctx, &grpc_ops.CreateRequest{
		DatabaseMetaInfo: databaseMetaInfo,
		Item: &grpc_ops.Item{
			Key:   ltngKey,
			Value: ltngValue,
		},
		IndexOpts: &grpc_ops.IndexOpts{
			HasIdx:       true,
			ParentKey:    ltngKey,
			IndexingKeys: [][]byte{ltngEmailIndexKey},
		},
		RetrialOpts: nil,
	})
	end2 := time.Since(begin2)
	require.NoError(t, err)
	t.Log("set time ->", end2.Microseconds())
	//t.Log("Set response ->", setResp.String())

	begin3 := time.Now()
	_, err = ltngClient.Load(ctx, &grpc_ops.LoadRequest{
		DatabaseMetaInfo: databaseMetaInfo,
		Item:             &grpc_ops.Item{Key: ltngKey},
	})
	end3 := time.Since(begin3)
	require.NoError(t, err)
	t.Log("get time ->", end3.Microseconds())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin3a := time.Now()
	_, err = ltngClient.Load(ctx, &grpc_ops.LoadRequest{
		DatabaseMetaInfo: databaseMetaInfo,
		Item:             &grpc_ops.Item{Key: ltngKey},
		IndexOpts: &grpc_ops.IndexOpts{
			HasIdx:       true,
			IndexingKeys: [][]byte{ltngEmailIndexKey},
			IndexingProperties: &grpc_ops.IndexProperties{
				IndexSearchPattern: grpc_ops.IndexProperties_ONE,
			},
		},
	})
	end3a := time.Since(begin3a)
	require.NoError(t, err)
	t.Log("get time ->", end3a.Microseconds())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin4 := time.Now()
	listResp, err := ltngClient.List(
		ctx,
		&grpc_ops.ListRequest{
			DatabaseMetaInfo: databaseMetaInfo,
			Pagination: &grpc_pagination.Pagination{
				PageId:   1,
				PageSize: 5,
			},
		},
	)
	end4 := time.Since(begin4)
	require.NoError(t, err)
	t.Log("list time ->", end4.Microseconds())
	//_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//t.Log("List response ->", string(IndentedListResp))

	begin5 := time.Now()
	_, err = ltngClient.Delete(
		ctx,
		&grpc_ops.DeleteRequest{
			DatabaseMetaInfo: databaseMetaInfo,
			Item:             &grpc_ops.Item{Key: ltngKey},
			IndexOpts: &grpc_ops.IndexOpts{
				HasIdx: true,
				IndexingProperties: &grpc_ops.IndexProperties{
					IndexDeletionBehaviour: grpc_ops.IndexProperties_CASCADE,
				},
			},
		},
	)
	end5 := time.Since(begin5)
	require.NoError(t, err)
	t.Log("delete time ->", end5.Microseconds())
	//t.Log("Delete response ->", deleteResp.String())

	listResp, err = ltngClient.List(
		ctx,
		&grpc_ops.ListRequest{
			DatabaseMetaInfo: databaseMetaInfo,
			Pagination: &grpc_pagination.Pagination{
				PageId:   1,
				PageSize: 5,
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, listResp)
	//_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//t.Log("List response ->", string(IndentedListResp))
}

func Test_Postgresql_ServerWithClient(t *testing.T) {
	ctx := context.Background()

	begin := time.Now()
	user, err := userStore.GetUser(ctx, psqlPayload.UserID)
	end := time.Since(begin)
	require.Error(t, err)
	t.Log("get time ->", end.Microseconds())
	require.Empty(t, user)

	begin2 := time.Now()
	user, err = userStore.CreateUser(ctx, psqlPayload)
	end2 := time.Since(begin2)
	require.NoError(t, err)
	t.Log("set time ->", end2.Microseconds())
	//require.NotEmpty(t, user)
	// t.Log(user)

	begin1 := time.Now()
	user, err = userStore.GetUser(ctx, psqlPayload.UserID)
	end1 := time.Since(begin1)
	require.NoError(t, err)
	t.Log("get time ->", end1.Microseconds())
	require.NotEmpty(t, user)

	begin3 := time.Now()
	users, err := userStore.ListPaginatedUsers(
		ctx,
		sqlc_user_store.ListPaginatedUsersParams{
			Limit:  2,
			Offset: 1,
		},
	)
	end3 := time.Since(begin3)
	require.NoError(t, err)
	t.Log("list time ->", end3.Microseconds())
	require.NotEmpty(t, users)

	begin4 := time.Now()
	err = userStore.DeleteUser(ctx, psqlPayload.UserID)
	end4 := time.Since(begin4)
	require.NoError(t, err)
	t.Log("delete time ->", end4.Microseconds())

	user, err = userStore.GetUser(ctx, psqlPayload.UserID)
	require.Error(t, err)
	require.Empty(t, user)
}

func TestInSequence_WithTimer(t *testing.T) {
	defer ltngClient.Close()
	defer psqlDB.Close()

	runs := 10
	for i := 0; i < runs; i++ {
		t.Run("ltng",
			func(t *testing.T) {
				Test_LightningNode_ServerWithClient(t)
			},
		)

		t.Run("psql",
			func(t *testing.T) {
				Test_Postgresql_ServerWithClient(t)
			},
		)
	}
}
