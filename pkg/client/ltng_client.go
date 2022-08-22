package client

import (
	"fmt"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	LTNGClient interface {
		grpc_mngmt.ManagementClient
		grpc_ops.OperationClient
		Close() error
	}
)

type ltng struct {
	grpc_mngmt.ManagementClient
	grpc_ops.OperationClient
	mmgrConn *grpc.ClientConn
	oprtConn *grpc.ClientConn
}

func NewLTNGClient(
	addresses *Addresses,
) (LTNGClient, error) {
	client := &ltng{}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: add cert opts
	}

	managerConn, err := grpc.Dial(addresses.Manager, opts...)
	client.mmgrConn = managerConn
	if err != nil {
		return nil, err
	}

	manager := grpc_mngmt.NewManagementClient(managerConn)
	client.ManagementClient = manager

	operatorConn, err := grpc.Dial(addresses.Operator, opts...)
	client.oprtConn = operatorConn
	if err != nil {
		return nil, err
	}

	operator := grpc_ops.NewOperationClient(operatorConn)
	client.OperationClient = operator

	return client, nil
}

func (c *ltng) Close() error {
	var err error
	mngrErr := c.mmgrConn.Close()
	if mngrErr != nil {
		err = mngrErr
	}
	oprtErr := c.oprtConn.Close()
	if oprtErr != nil {
		if mngrErr != nil {
			err = fmt.Errorf("%v - %v", mngrErr, oprtErr)
		} else {
			err = oprtErr
		}
	}

	return err
}
