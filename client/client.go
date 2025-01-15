package ltng_client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Params struct {
		Address string
		Engine  string
	}

	Client interface {
		grpc_ltngdb.LightningDBClient
		Close() error
	}

	ltng struct {
		grpc_ltngdb.LightningDBClient
		ltngDBClientConn *grpc.ClientConn
	}
)

func (p *Params) IsValid() bool {
	return p == nil || p.Address == "" // || p.Engine == ""
}

func New(
	ctx context.Context,
	params *Params,
) (Client, error) {
	if params.IsValid() {
		return nil, fmt.Errorf("invalid params %v", params)
	}

	client := &ltng{}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TODO: add cert opts
	}

	grpcClientConn, err := grpc.NewClient(params.Address, opts...)
	if err != nil {
		return nil, err
	}

	client.ltngDBClientConn = grpcClientConn

	lightningDBClient := grpc_ltngdb.NewLightningDBClient(grpcClientConn)

	client.LightningDBClient = lightningDBClient

	//_, err = client.LightningDBClient.CheckLightingNodeEngine(ctx,
	//	&grpc_query_config.CheckEngineRequest{
	//		Engine: params.Engine,
	//	})
	//if err != nil {
	//	return nil, fmt.Errorf("failed to check ltng's engine: %v", err)
	//}

	//time.Sleep(time.Second * 1)

	return client, nil
}

func (c *ltng) Close() error {
	return c.ltngDBClientConn.Close()
}
