package client

import (
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	LTNGClient interface {
		grpc_mngmt.ManagementClient
		grpc_ops.OperationClient
	}
)

type ltng struct {
	grpc_mngmt.ManagementClient
	grpc_ops.OperationClient
}

func NewLTNGClient(
	addresses *Addresses,
) (LTNGClient, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: add cert opts
	}

	managerConn, err := grpc.Dial(addresses.Manager, opts...)
	defer managerConn.Close()
	if err != nil {
		return nil, err
	}

	manager := grpc_mngmt.NewManagementClient(managerConn)

	operatorConn, err := grpc.Dial(addresses.Operator, opts...)
	defer managerConn.Close()
	if err != nil {
		return nil, err
	}

	operator := grpc_ops.NewOperationClient(operatorConn)

	client := &ltng{
		ManagementClient: manager,
		OperationClient:  operator,
	}

	return client, nil
}
