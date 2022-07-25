package client_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/search"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

const (
	managerServerAddr  = "localhost:50051"
	operatorServerAddr = "localhost:50052"

	clientTestStore     = "client-test-store"
	clientTestStorePath = "client_test_store"
)

func Test_LightningNode_ServerWithClient(t *testing.T) {
	ctx := context.Background()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	managerConn, err := grpc.Dial(managerServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	manager := grpc_mngmt.NewManagementClient(managerConn)

	stores, err := manager.ListStores(ctx, &grpc_mngmt.ListStoresRequest{
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	t.Log("stores ->", stores)

	createdStoreResp, err := manager.CreateStore(ctx, &grpc_mngmt.CreateStoreRequest{
		Name: clientTestStore,
		Path: clientTestStorePath,
	})
	require.NoError(t, err)
	t.Log("created store if it does not exist ->", createdStoreResp)

	operatorConn, err := grpc.Dial(operatorServerAddr, opts...)
	require.NoError(t, err)
	defer managerConn.Close()

	operator := grpc_ops.NewOperationClient(operatorConn)

	keyToStore := []byte("any-test-key")
	valueToStore := []byte("any-test-value")

	getResp, err := operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.Error(t, err)
	t.Log("Get response string ->", getResp.String())
	t.Log("Get response ->", string(getResp.GetValue()))

	setResp, err := operator.Set(ctx, &grpc_ops.SetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Item: &grpc_ops.Item{
			Key:   keyToStore,
			Value: valueToStore,
		},
	})
	require.NoError(t, err)
	t.Log("Set response ->", setResp.String())

	getResp, err = operator.Get(ctx, &grpc_ops.GetRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.NoError(t, err)
	t.Log("Get response ->", string(getResp.GetValue()))

	listResp, err := operator.List(ctx, &grpc_ops.ListRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Pagination: &grpc_pagination.Pagination{
			PageId:   1,
			PageSize: 5,
		},
	})
	require.NoError(t, err)
	IndentedListResp, err := json.MarshalIndent(listResp.GetItems(), "", "  ")
	t.Log("List response ->", string(IndentedListResp))

	deleteResp, err := operator.Delete(ctx, &grpc_ops.DeleteRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
		Key:              keyToStore,
	})
	require.NoError(t, err)
	t.Log("Delete response ->", deleteResp.String())

	listResp, err = operator.List(ctx, &grpc_ops.ListRequest{
		DatabaseMetaInfo: &grpc_ops.DatabaseMetaInfo{DatabaseName: clientTestStore},
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
