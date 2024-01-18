package ltng_client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpc_query_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/common/queries/config"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

type (
	LTNGClientParams struct {
		Addresses *Addresses
		Engine    string
	}

	LTNGClient interface {
		grpc_mngmt.ManagementClient
		grpc_ops.OperationClient
		Close() error
	}

	ltng struct {
		grpc_mngmt.ManagementClient
		grpc_ops.OperationClient
		mmgrConn *grpc.ClientConn
		oprtConn *grpc.ClientConn
	}
)

func (p *LTNGClientParams) IsValid() bool {
	return p == nil || p.Addresses == nil // || p.Engine == ""
}

func NewLTNGClient(
	ctx context.Context,
	params *LTNGClientParams,
) (LTNGClient, error) {
	if params.IsValid() {
		return nil, fmt.Errorf("invalid params %v", params)
	}

	client := &ltng{}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: add cert opts
	}

	managerConn, err := grpc.Dial(params.Addresses.Manager, opts...)
	client.mmgrConn = managerConn
	if err != nil {
		return nil, err
	}

	manager := grpc_mngmt.NewManagementClient(managerConn)
	client.ManagementClient = manager

	operatorConn, err := grpc.Dial(params.Addresses.Operator, opts...)
	client.oprtConn = operatorConn
	if err != nil {
		return nil, err
	}

	operator := grpc_ops.NewOperationClient(operatorConn)
	client.OperationClient = operator

	_, err = manager.CheckManagerEngine(ctx, &grpc_query_config.CheckEngineRequest{
		Engine: params.Engine,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check manager's engine: %v", err)
	}

	_, err = operator.CheckOperatorEngine(ctx, &grpc_query_config.CheckEngineRequest{
		Engine: params.Engine,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check operator's engine: %v", err)
	}

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
