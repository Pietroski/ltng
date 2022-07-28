package indexed

import (
	"context"
	"database/sql"
	"encoding/json"
	grpc_indexed_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/pkg/tools/tracer"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/search"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/tests/benchmark/lightning-db_vs_postgresql/config"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sqlc_user_store "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/tests/benchmark/lightning-db_vs_postgresql-indexing/internal/adaptors/datastore/postgresql/user/sqlc"
)

const (
	indexedManagerServerAddr  = "localhost:50053"
	indexedOperatorServerAddr = "localhost:50054"

	clientTestStore     = "client-test-store"
	clientTestStorePath = "client_test_store"
)

func Test_LightningNode_ServerWithClient(t *testing.T) {
	type CreateUserParams struct {
		UserID    uuid.UUID `json:"userID"`
		Email     string    `json:"email"`
		Name      string    `json:"name"`
		Surname   string    `json:"surname"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}
	payload := CreateUserParams{
		UserID:    uuid.New(),
		Email:     "any.email@email.com",
		Name:      "any-name",
		Surname:   "any-surname",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	ctx := context.Background()
	ctx = go_tracer.NewCtxTracer().Trace(ctx)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(indexedManagerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	indexedManager := grpc_indexed_mngmt.NewIndexedManagementClient(managerConn)

	_, err = indexedManager.ListIndexedStores(ctx, &grpc_indexed_mngmt.ListIndexedStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	//t.Log("stores ->", stores)

	_, err = indexedManager.CreateIndexedStore(ctx, &grpc_indexed_mngmt.CreateIndexedStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	//t.Log("created store if it does not exist ->", createdStoreResp)

	indexedOperatorConn, err := grpc.Dial(indexedOperatorServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	indexedOperator := grpc_indexed_ops.NewIndexedOperationClient(indexedOperatorConn)

	//keyToStore := []byte("any-test-key")
	//valueToStore := []byte("any-test-value")

	serializer := go_serializer.NewJsonSerializer()
	keyToStore, err := serializer.Serialize(payload.UserID)
	require.NoError(t, err)
	valueToStore, err := serializer.Serialize(payload)
	require.NoError(t, err)

	indexedKey, err := serializer.Serialize(payload.Email)
	require.NoError(t, err)

	begin := time.Now()
	_, err = indexedOperator.IndexedGet(ctx, &grpc_indexed_ops.GetIndexedRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item:             &grpc_ops.Item{},
		Index: &grpc_indexed_ops.Index{
			ShallIndex: true,
			ParentKey:  nil,
			IndexKeys:  [][]byte{indexedKey},
			IndexProperties: &grpc_indexed_ops.IndexProperties{
				IndexSearchPattern: grpc_indexed_ops.IndexProperties_ONE,
			},
		},
	})
	end := time.Since(begin)
	require.Error(t, err)
	t.Log("get time ->", end.Microseconds())
	//t.Log("Get response string ->", getResp.String())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin2 := time.Now()
	_, err = indexedOperator.IndexedSet(ctx, &grpc_indexed_ops.SetIndexedRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ops.Item{
			Key:   keyToStore,
			Value: valueToStore,
		},
		Index: &grpc_indexed_ops.Index{
			ShallIndex:      true,
			ParentKey:       keyToStore,
			IndexKeys:       [][]byte{indexedKey},
			IndexProperties: nil,
		},
	})
	end2 := time.Since(begin2)
	require.NoError(t, err)
	t.Log("set time ->", end2.Microseconds())
	//t.Log("Set response ->", setResp.String())

	begin3 := time.Now()
	_, err = indexedOperator.IndexedGet(ctx, &grpc_indexed_ops.GetIndexedRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item:             &grpc_ops.Item{},
		Index: &grpc_indexed_ops.Index{
			ShallIndex: true,
			ParentKey:  nil,
			IndexKeys:  [][]byte{indexedKey},
			IndexProperties: &grpc_indexed_ops.IndexProperties{
				IndexSearchPattern: grpc_indexed_ops.IndexProperties_ONE,
			},
		},
	})
	end3 := time.Since(begin3)
	require.NoError(t, err)
	t.Log("get time ->", end3.Microseconds())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin4 := time.Now()
	listResp, err := indexedOperator.IndexedList(ctx, &grpc_indexed_ops.ListIndexedRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	end4 := time.Since(begin4)
	require.NoError(t, err)
	t.Log("list time ->", end4.Microseconds())
	_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//t.Log("List response ->", string(IndentedListResp))

	begin5 := time.Now()
	_, err = indexedOperator.IndexedDelete(ctx, &grpc_indexed_ops.DeleteIndexedRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ops.Item{
			Key: keyToStore,
		},
		Index: &grpc_indexed_ops.Index{
			ShallIndex: true,
			ParentKey:  nil,
			IndexKeys:  [][]byte{indexedKey},
			IndexProperties: &grpc_indexed_ops.IndexProperties{
				IndexDeletionBehaviour: grpc_indexed_ops.IndexProperties_CASCADE,
			},
		},
	})
	end5 := time.Since(begin5)
	require.NoError(t, err)
	t.Log("delete time ->", end5.Microseconds())
	//t.Log("Delete response ->", deleteResp.String())

	listResp, err = indexedOperator.IndexedList(ctx, &grpc_indexed_ops.ListIndexedRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	// require.Len(t, listResp.GetItems(), 5)
	_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//t.Log("List response ->", string(IndentedListResp))
}

func Test_Postgresql_ServerWithClient(t *testing.T) {
	ctx := context.Background()
	ctx = go_tracer.NewCtxTracer().Trace(ctx)

	cfg := &ltng_node_config.Config{}
	err := go_env_extractor.LoadEnvs(cfg)
	require.NoError(t, err)

	conn, err := sql.Open(cfg.PostgreSQL.DriverName, cfg.PostgreSQL.DataSourceName)
	require.NoError(t, err)
	defer conn.Close()

	userStore := sqlc_user_store.NewUserStore(conn)

	payload := sqlc_user_store.CreateUserParams{
		UserID:  uuid.New(),
		Email:   "any.email2@email.com",
		Name:    "any-name",
		Surname: "any-surname",
		CreatedAt: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
		UpdatedAt: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}

	begin := time.Now()
	user, err := userStore.GetUserByEmail(ctx, payload.Email)
	end := time.Since(begin)
	require.Error(t, err)
	t.Log("get time ->", end.Microseconds())
	require.Empty(t, user)

	begin2 := time.Now()
	user, err = userStore.CreateUser(ctx, payload)
	end2 := time.Since(begin2)
	require.NoError(t, err)
	t.Log("set time ->", end2.Microseconds())
	//require.NotEmpty(t, user)
	// t.Log(user)

	begin1 := time.Now()
	user, err = userStore.GetUserByEmail(ctx, payload.Email)
	end1 := time.Since(begin1)
	require.NoError(t, err)
	t.Log("get time ->", end1.Microseconds())
	require.NotEmpty(t, user)

	begin3 := time.Now()
	users, err := userStore.ListPaginatedUsers(ctx, sqlc_user_store.ListPaginatedUsersParams{
		Limit:  2,
		Offset: 1,
	})
	end3 := time.Since(begin3)
	require.NoError(t, err)
	t.Log("list time ->", end3.Microseconds())
	require.NotEmpty(t, users)

	begin4 := time.Now()
	err = userStore.DeleteUser(ctx, payload.UserID)
	end4 := time.Since(begin4)
	require.NoError(t, err)
	t.Log("delete time ->", end4.Microseconds())

	user, err = userStore.GetUser(ctx, payload.UserID)
	require.Error(t, err)
	require.Empty(t, user)
}

func Test_Timer_Comparison(t *testing.T) {
	t.Run(
		"lightning-db not indexed tables",
		func(t *testing.T) {
			t.Log("lightning-db")
			Test_LightningNode_ServerWithClient(t)
			t.Log("postgresql")
			Test_Postgresql_ServerWithClient(t)
		},
	)
}
