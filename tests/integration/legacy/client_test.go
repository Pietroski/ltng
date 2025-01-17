package legacy_test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"

	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

const (
	managerServerAddr  = "localhost:50051"
	operatorServerAddr = "localhost:50052"

	clientTestStore     = "tests-integration-client-test-store"
	clientTestStorePath = "tests/integration/client_test_store"
)

func Test_LightningNode_ServerWithClient(t *testing.T) {
	ctx := context.Background()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(managerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	manager := grpc_ltngdb.NewLightningDBClient(managerConn)

	stores, err := manager.ListStores(ctx, &grpc_ltngdb.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	t.Log("stores ->", stores)

	createdStoreResp, err := manager.CreateStore(ctx, &grpc_ltngdb.CreateStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	t.Log("created store if it does not exist ->", createdStoreResp)

	operatorConn, err := grpc.Dial(operatorServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	operator := grpc_ltngdb.NewLightningDBClient(operatorConn)

	keyToStore := []byte("any-test-key")
	valueToStore := []byte("any-test-value")

	getResp, err := operator.Load(ctx, &grpc_ltngdb.LoadRequest{
		DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item:             &grpc_ltngdb.Item{Key: keyToStore},
	})
	require.Error(t, err)
	t.Log("Get response string ->", getResp.String())
	t.Log("Get response ->", string(getResp.GetValue()))

	setResp, err := operator.Create(ctx, &grpc_ltngdb.CreateRequest{
		DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ltngdb.Item{
			Key:   keyToStore,
			Value: valueToStore,
		},
	})
	require.NoError(t, err)
	t.Log("Set response ->", setResp.String())

	getResp, err = operator.Load(ctx, &grpc_ltngdb.LoadRequest{
		DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item:             &grpc_ltngdb.Item{Key: keyToStore},
	})
	require.NoError(t, err)
	t.Log("Get response ->", string(getResp.GetValue()))

	listResp, err := operator.List(ctx, &grpc_ltngdb.ListRequest{
		DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	IndentedListResp, err := json.MarshalIndent(listResp.GetItems(), "", "  ")
	t.Log("List response ->", string(IndentedListResp))

	deleteResp, err := operator.Delete(ctx, &grpc_ltngdb.DeleteRequest{
		DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item:             &grpc_ltngdb.Item{Key: keyToStore},
	})
	require.NoError(t, err)
	t.Log("Delete response ->", deleteResp.String())

	listResp, err = operator.List(ctx, &grpc_ltngdb.ListRequest{
		DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	// require.Len(t, listResp.GetItems(), 5)
	IndentedListResp, err = json.MarshalIndent(listResp.GetItems(), "", "  ")
	t.Log("List response ->", string(IndentedListResp))
}

func Test_LightningNode_ServerWithClient_TracingTest(t *testing.T) {
	ctx := context.Background()

	tracer := go_tracer.NewCtxTracer()

	var err error
	ctx, err = tracer.Trace(ctx)
	if err != nil {
		log.Fatalf("failed to create trace id: %v", err)
		os.Exit(1)
		return
	}

	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: false,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)
	logger = logger.FromCtx(ctx)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(managerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	manager := grpc_ltngdb.NewLightningDBClient(managerConn)

	stores, err := manager.ListStores(ctx, &grpc_ltngdb.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	t.Log("stores ->", stores)

	createdStoreResp, err := manager.CreateStore(ctx, &grpc_ltngdb.CreateStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	t.Log("created store if it does not exist ->", createdStoreResp)

	stores, err = manager.ListStores(ctx, &grpc_ltngdb.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	t.Log("stores ->", stores)
}
