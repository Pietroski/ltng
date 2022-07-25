package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/schemas/generated/go/common/search"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/schemas/generated/go/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/schemas/generated/go/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/tests/benchmark/lightning-db_vs_postgresql/config"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"

	sqlc_user_store "gitlab.com/pietroski-software-company/lightning-db/lightning-node/tests/benchmark/lightning-db_vs_postgresql/internal/adaptors/datastore/postgresql/user/sqlc"
)

const (
	managerServerAddr  = "localhost:50051"
	operatorServerAddr = "localhost:50052"

	clientTestStore     = "client-test-store"
	clientTestStorePath = "client_test_store"
)

func Test_LightningNode_ServerWithClientPre(t *testing.T) {
	t.Skip()
	ctx := context.Background()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(managerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	manager := grpc_mngmt.NewManagementClient(managerConn)

	_, err = manager.ListStores(ctx, &grpc_mngmt.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	//t.Log("stores ->", stores)

	_, err = manager.CreateStore(ctx, &grpc_mngmt.CreateStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	//t.Log("created store if it does not exist ->", createdStoreResp)

	operatorConn, err := grpc.Dial(operatorServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	operator := grpc_ops.NewOperationClient(operatorConn)

	keyToStore := []byte("any-test-key")
	valueToStore := []byte("any-test-value")

	_, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.Error(t, err)
	//t.Log("Get response string ->", getResp.String())
	//t.Log("Get response ->", string(getResp.GetValue()))

	_, err = operator.Set(ctx, &grpc_ops.SetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ops.Item{
			Key:   keyToStore,
			Value: valueToStore,
		},
	})
	require.NoError(t, err)
	//t.Log("Set response ->", setResp.String())

	_, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.NoError(t, err)
	//t.Log("Get response ->", string(getResp.GetValue()))

	listResp, err := operator.List(ctx, &grpc_ops.ListRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//t.Log("List response ->", string(IndentedListResp))

	_, err = operator.Delete(ctx, &grpc_ops.DeleteRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.NoError(t, err)
	//t.Log("Delete response ->", deleteResp.String())

	listResp, err = operator.List(ctx, &grpc_ops.ListRequest{
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

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(managerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	manager := grpc_mngmt.NewManagementClient(managerConn)

	_, err = manager.ListStores(ctx, &grpc_mngmt.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	//t.Log("stores ->", stores)

	_, err = manager.CreateStore(ctx, &grpc_mngmt.CreateStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	//t.Log("created store if it does not exist ->", createdStoreResp)

	operatorConn, err := grpc.Dial(operatorServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	operator := grpc_ops.NewOperationClient(operatorConn)

	//keyToStore := []byte("any-test-key")
	//valueToStore := []byte("any-test-value")

	serializer := go_serializer.NewJsonSerializer()
	keyToStore, err := serializer.Serialize(payload.UserID)
	require.NoError(t, err)
	valueToStore, err := serializer.Serialize(payload)
	require.NoError(t, err)

	begin := time.Now()
	_, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	end := time.Since(begin)
	require.Error(t, err)
	t.Log("get time ->", end.Microseconds())
	//t.Log("Get response string ->", getResp.String())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin2 := time.Now()
	_, err = operator.Set(ctx, &grpc_ops.SetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ops.Item{
			Key:   keyToStore,
			Value: valueToStore,
		},
	})
	end2 := time.Since(begin2)
	require.NoError(t, err)
	t.Log("set time ->", end2.Microseconds())
	//t.Log("Set response ->", setResp.String())

	begin3 := time.Now()
	_, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	end3 := time.Since(begin3)
	require.NoError(t, err)
	t.Log("get time ->", end3.Microseconds())
	//t.Log("Get response ->", string(getResp.GetValue()))

	begin4 := time.Now()
	listResp, err := operator.List(ctx, &grpc_ops.ListRequest{
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
	_, err = operator.Delete(ctx, &grpc_ops.DeleteRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	end5 := time.Since(begin5)
	require.NoError(t, err)
	t.Log("delete time ->", end5.Microseconds())
	//t.Log("Delete response ->", deleteResp.String())

	listResp, err = operator.List(ctx, &grpc_ops.ListRequest{
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
	user, err := userStore.GetUser(ctx, payload.UserID)
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
	user, err = userStore.GetUser(ctx, payload.UserID)
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

func Test_LightningNode_ServerWithClientNoTimer(t *testing.T) {
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

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(managerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	manager := grpc_mngmt.NewManagementClient(managerConn)

	_, err = manager.ListStores(ctx, &grpc_mngmt.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	//t.Log("stores ->", stores)

	_, err = manager.CreateStore(ctx, &grpc_mngmt.CreateStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	//t.Log("created store if it does not exist ->", createdStoreResp)

	operatorConn, err := grpc.Dial(operatorServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	operator := grpc_ops.NewOperationClient(operatorConn)

	//keyToStore := []byte("any-test-key")
	//valueToStore := []byte("any-test-value")

	serializer := go_serializer.NewJsonSerializer()
	keyToStore, err := serializer.Serialize(payload.UserID)
	require.NoError(t, err)
	valueToStore, err := serializer.Serialize(payload)
	require.NoError(t, err)

	_, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.Error(t, err)
	//t.Log("Get response string ->", getResp.String())
	//t.Log("Get response ->", string(getResp.GetValue()))

	_, err = operator.Set(ctx, &grpc_ops.SetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ops.Item{
			Key:   keyToStore,
			Value: valueToStore,
		},
	})
	require.NoError(t, err)
	//t.Log("Set response ->", setResp.String())

	_, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.NoError(t, err)
	//t.Log("Get response ->", string(getResp.GetValue()))

	listResp, err := operator.List(ctx, &grpc_ops.ListRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	_, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	//t.Log("List response ->", string(IndentedListResp))

	_, err = operator.Delete(ctx, &grpc_ops.DeleteRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.NoError(t, err)
	//t.Log("Delete response ->", deleteResp.String())

	listResp, err = operator.List(ctx, &grpc_ops.ListRequest{
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

func Test_Postgresql_ServerWithClientNoTimer(t *testing.T) {
	ctx := context.Background()

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

	user, err := userStore.GetUser(ctx, payload.UserID)
	require.Error(t, err)
	require.Empty(t, user)

	user, err = userStore.CreateUser(ctx, payload)
	require.NoError(t, err)
	//require.NotEmpty(t, user)
	// t.Log(user)

	user, err = userStore.GetUser(ctx, payload.UserID)
	require.NoError(t, err)
	require.NotEmpty(t, user)

	users, err := userStore.ListPaginatedUsers(ctx, sqlc_user_store.ListPaginatedUsersParams{
		Limit:  2,
		Offset: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, users)

	err = userStore.DeleteUser(ctx, payload.UserID)
	require.NoError(t, err)

	_, err = userStore.ListPaginatedUsers(ctx, sqlc_user_store.ListPaginatedUsersParams{
		Limit:  2,
		Offset: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, users)

	user, err = userStore.GetUser(ctx, payload.UserID)
	require.Error(t, err)
	require.Empty(t, user)
}

func Test_Benchmark_LightningNode_ServerWithClient(t *testing.T) {
	limit := 1000
	begin := time.Now()
	for i := 0; i < limit; i++ {
		Test_LightningNode_ServerWithClientNoTimer(t)
	}
	end := time.Since(begin)
	t.Log("total time ->", end.Seconds())
}

func Test_Benchmark_Postgresql_ServerWithClient(t *testing.T) {
	limit := 1000
	begin := time.Now()
	for i := 0; i < limit; i++ {
		Test_Postgresql_ServerWithClientNoTimer(t)
	}
	end := time.Since(begin)
	t.Log("total time ->", end.Seconds())
}
