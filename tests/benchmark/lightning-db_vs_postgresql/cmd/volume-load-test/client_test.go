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

	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	ltng_client "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/client"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/search"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/tests/benchmark/lightning-db_vs_postgresql/config"
	sqlc_user_store "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/tests/benchmark/lightning-db_vs_postgresql/internal/adaptors/datastore/postgresql/user/sqlc"
)

const (
	managerServerAddr  = "localhost:50051"
	operatorServerAddr = "localhost:50052"

	clientTestStore     = "volume-load-test-client-test-store"
	clientTestStorePath = "volume/load/test/client_test_store"

	//population = 1_000
	//
	//pageID   = 50 population / 20
	//pageSize = 10 population / 10

	population = 50

	pageID   = population / 2
	pageSize = population / 10
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

	LTNGEntry struct {
		ltngKey           []byte
		ltngValue         []byte
		ltngEmailIndexKey []byte
	}

	LTNGEntries []*LTNGEntry
	PSQLEntries []*sqlc_user_store.CreateUserParams
)

var (
	ltngClient ltng_client.LTNGClient

	psqlDB    *sql.DB
	userStore sqlc_user_store.Store

	databaseMetaInfo = &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore}
	serializer       = go_serializer.NewJsonSerializer()

	ltngEntries = make(LTNGEntries, population)
	psqlEntries = make(PSQLEntries, population)

	page_id = func() func() uint64 {
		counter := uint64(0)
		return func() uint64 {
			counter++
			return counter
		}
	}
)

func initLTNG(ctx context.Context, cfg *ltng_node_config.Config) (err error) {
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

	return
}

func initPSQL(ctx context.Context, cfg *ltng_node_config.Config) (err error) {
	psqlDB, err = sql.Open(cfg.PostgreSQL.DriverName, cfg.PostgreSQL.DataSourceName)
	if err != nil {
		defer psqlDB.Close()
		log.Fatalf("failed to start psql client: %v", err)
	}
	userStore = sqlc_user_store.NewUserStore(psqlDB)

	return
}

func populateDBs() (err error) {
	for idx := 0; idx < population; idx++ {
		payload := &CreateUserParams{
			UserID:    uuid.New(),
			Email:     go_random.RandomEmail(),
			Name:      go_random.RandomString(12),
			Surname:   go_random.RandomString(12),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		ltngKey, err := serializer.Serialize(payload.UserID)
		if err != nil {
			defer ltngClient.Close()
			log.Fatalf("failed to serialize ltng key: %v", err)
		}
		ltngValue, err := serializer.Serialize(payload)
		if err != nil {
			defer ltngClient.Close()
			log.Fatalf("failed to serialize ltng value: %v", err)
		}
		ltngEmailIndexKey, err := serializer.Serialize(payload.Email)
		if err != nil {
			defer ltngClient.Close()
			log.Fatalf("failed to serialize ltng email index key: %v", err)
		}

		entry := &LTNGEntry{
			ltngKey:           ltngKey,
			ltngValue:         ltngValue,
			ltngEmailIndexKey: ltngEmailIndexKey,
		}

		psqlPayload := &sqlc_user_store.CreateUserParams{
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

		ltngEntries[idx] = entry
		psqlEntries[idx] = psqlPayload
	}

	return
}

func init() {
	ctx := context.Background()

	cfg := &ltng_node_config.Config{}
	err := go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		log.Fatalf("failed to get config: %v", err)
	}

	if err = initLTNG(ctx, cfg); err != nil {
		log.Fatalf("failed to init ltng: %v", err)
	}

	if err = initPSQL(ctx, cfg); err != nil {
		log.Fatalf("failed to init psql: %v", err)
	}

	if err = populateDBs(); err != nil {
		log.Fatalf("failed to populate DB's: %v", err)
	}
}

func Test_LightningNode_ServerWithClient(t *testing.T) {
	ctx := context.Background()
	var err error

	pager := page_id()

	// load
	{
		begin := time.Now()
		_, err = ltngClient.Load(
			ctx,
			&grpc_ops.LoadRequest{
				DatabaseMetaInfo: databaseMetaInfo,
				Item:             &grpc_ops.Item{Key: ltngEntries[0].ltngKey},
			},
		)
		end := time.Since(begin)
		require.Error(t, err)
		t.Log("get time ->", end.Microseconds())
		//t.Log("Get response string ->", getResp.String())
		//t.Log("Get response ->", string(getResp.GetValue()))
	}

	// create
	{
		begin := time.Now()
		for _, item := range ltngEntries {
			_, err = ltngClient.Create(ctx, &grpc_ops.CreateRequest{
				DatabaseMetaInfo: databaseMetaInfo,
				Item: &grpc_ops.Item{
					Key:   item.ltngKey,
					Value: item.ltngValue,
				},
				IndexOpts: &grpc_ops.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.ltngKey,
					IndexingKeys: [][]byte{item.ltngEmailIndexKey},
				},
				RetrialOpts: nil,
			})
		}
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("set time ->", end.Seconds())
	}

	// load specific
	{
		begin := time.Now()
		_, err = ltngClient.Load(ctx, &grpc_ops.LoadRequest{
			DatabaseMetaInfo: databaseMetaInfo,
			Item:             &grpc_ops.Item{Key: ltngEntries[population/2].ltngKey},
		})
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("get time ->", end.Microseconds())
		//t.Log("Get response ->", string(getResp.GetValue()))
	}

	// load specific from index
	{
		begin := time.Now()
		_, err = ltngClient.Load(ctx, &grpc_ops.LoadRequest{
			DatabaseMetaInfo: databaseMetaInfo,
			Item:             &grpc_ops.Item{Key: ltngEntries[population/2].ltngKey},
			IndexOpts: &grpc_ops.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{ltngEntries[population/2].ltngEmailIndexKey},
				IndexingProperties: &grpc_ops.IndexProperties{
					IndexSearchPattern: grpc_ops.IndexProperties_ONE,
				},
			},
		})
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("get time ->", end.Microseconds())
		//t.Log("Get response ->", string(getResp.GetValue()))
	}

	// list paginated
	{
		begin := time.Now()
		_, err := ltngClient.List(
			ctx,
			&grpc_ops.ListRequest{
				DatabaseMetaInfo: databaseMetaInfo,
				Pagination: &grpc_pagination.Pagination{
					PageId:   pager(),
					PageSize: pageSize,
				},
			},
		)
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("list time ->", end.Microseconds())
		//t.Log("list len ->", len(list.Items))
		//_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
		//t.Log("List response ->", string(IndentedListResp))
	}

	// list all
	//{
	//	begin := time.Now()
	//	listResp, err := ltngClient.List(
	//		ctx,
	//		&grpc_ops.ListRequest{
	//			DatabaseMetaInfo: databaseMetaInfo,
	//			IndexOpts: &grpc_ops.IndexOpts{
	//				HasIdx: true,
	//				IndexingProperties: &grpc_ops.IndexProperties{
	//					ListSearchPattern: grpc_ops.IndexProperties_ALL,
	//				},
	//			},
	//			Pagination: &grpc_pagination.Pagination{},
	//		},
	//	)
	//	end := time.Since(begin)
	//	require.NoError(t, err)
	//	t.Log("list time ->", end.Microseconds())
	//	require.NotNil(t, listResp)
	//	//_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//	//t.Log("List response ->", string(IndentedListResp))
	//}

	// delete all on cascade
	{
		begin := time.Now()
		for _, item := range ltngEntries {
			_, err = ltngClient.Delete(
				ctx,
				&grpc_ops.DeleteRequest{
					DatabaseMetaInfo: databaseMetaInfo,
					Item:             &grpc_ops.Item{Key: item.ltngKey},
					IndexOpts: &grpc_ops.IndexOpts{
						HasIdx: true,
						IndexingProperties: &grpc_ops.IndexProperties{
							IndexDeletionBehaviour: grpc_ops.IndexProperties_CASCADE,
						},
					},
				},
			)
		}
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("delete time ->", end.Seconds())
	}

	// list paginated empty
	{
		begin := time.Now()
		_, err := ltngClient.List(
			ctx,
			&grpc_ops.ListRequest{
				DatabaseMetaInfo: databaseMetaInfo,
				Pagination: &grpc_pagination.Pagination{
					PageId:   pager(),
					PageSize: pageSize,
				},
			},
		)
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("list time ->", end.Microseconds())
		//_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
		//t.Log("List response ->", string(IndentedListResp))
	}

	//list all empty
	{
		begin := time.Now()
		_, err := ltngClient.List(
			ctx,
			&grpc_ops.ListRequest{
				DatabaseMetaInfo: databaseMetaInfo,
				IndexOpts: &grpc_ops.IndexOpts{
					HasIdx: true,
					IndexingProperties: &grpc_ops.IndexProperties{
						ListSearchPattern: grpc_ops.IndexProperties_ALL,
					},
				},
				Pagination: &grpc_pagination.Pagination{},
			},
		)
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("list time ->", end.Microseconds())
		//t.Log("list len ->", len(list.Items))
		//_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
		//t.Log("List response ->", string(IndentedListResp))
	}
}

func Test_Postgresql_ServerWithClient(t *testing.T) {
	ctx := context.Background()

	pager := page_id()

	// load first
	{
		begin := time.Now()
		user, err := userStore.GetUser(ctx, psqlEntries[0].UserID)
		end := time.Since(begin)
		require.Error(t, err)
		t.Log("get time ->", end.Microseconds())
		require.Empty(t, user)
	}

	// create chunk
	{
		begin := time.Now()
		for _, item := range psqlEntries {
			_, err := userStore.CreateUser(ctx, *item)
			require.NoError(t, err)
		}
		end := time.Since(begin)
		t.Log("set time ->", end.Seconds())
		//require.NotEmpty(t, user)
		// t.Log(user)
	}

	// load specific
	{
		begin := time.Now()
		_, err := userStore.GetUser(ctx, psqlEntries[population/500].UserID)
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("get time ->", end.Microseconds())
		//require.NotEmpty(t, user)
	}

	// load specific from index
	{
		begin := time.Now()
		_, err := userStore.GetUserByEmail(ctx, psqlEntries[population/500].Email)
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("get time ->", end.Microseconds())
		//require.NotEmpty(t, user)
	}

	// load paginated
	{
		begin := time.Now()
		_, err := userStore.ListPaginatedUsers(
			ctx,
			sqlc_user_store.ListPaginatedUsersParams{
				Limit:  pageSize,
				Offset: pageSize * (int32(pager()) - 1),
			},
		)
		end := time.Since(begin)
		require.NoError(t, err)
		t.Log("list time ->", end.Microseconds())
		//require.NotEmpty(t, users)
	}

	// load all
	//{
	//	begin := time.Now()
	//	_, err := userStore.ListUsers(ctx)
	//	end := time.Since(begin)
	//	require.NoError(t, err)
	//	t.Log("list time ->", end.Microseconds())
	//	//require.NotEmpty(t, users)
	//}

	// delete chunk
	{
		begin := time.Now()
		for _, item := range psqlEntries {
			err := userStore.DeleteUser(ctx, item.UserID)
			require.NoError(t, err)
		}
		end := time.Since(begin)
		t.Log("delete time ->", end.Seconds())
		//require.NotEmpty(t, user)
		// t.Log(user)
	}

	// load paginated
	{
		begin := time.Now()
		_, _ = userStore.ListPaginatedUsers(
			ctx,
			sqlc_user_store.ListPaginatedUsersParams{
				Limit:  pageSize,
				Offset: pageSize * (int32(pager()) - 1),
			},
		)
		end := time.Since(begin)
		//require.Error(t, err)
		t.Log("list time ->", end.Microseconds())
		//t.Log(t, users)
	}

	// load all
	{
		begin := time.Now()
		_, _ = userStore.ListUsers(ctx)
		end := time.Since(begin)
		//require.Error(t, err)
		t.Log("list time ->", end.Microseconds())
		//t.Log(t, users)
	}
}

func TestInSequence_WithTimer(t *testing.T) {
	t.Log("ltng entries count", len(ltngEntries))
	t.Log("psql entries count", len(psqlEntries))
	defer ltngClient.Close()
	defer psqlDB.Close()

	runs := 2
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
