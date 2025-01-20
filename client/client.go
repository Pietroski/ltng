package ltng_client

import (
	"context"
	"fmt"
	"log"

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
		//grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	}

	grpcClientConn, err := grpc.NewClient(params.Address, opts...)
	if err != nil {
		return nil, err
	}

	log.Printf("Connection state: %s", grpcClientConn.GetState().String())

	client.ltngDBClientConn = grpcClientConn

	lightningDBClient := grpc_ltngdb.NewLightningDBClient(grpcClientConn)

	client.LightningDBClient = lightningDBClient

	//// Monitor connection state changes
	//go func() {
	//	for {
	//		state := grpcClientConn.GetState()
	//		log.Printf("Connection state changed to: %s", state)
	//		if !grpcClientConn.WaitForStateChange(ctx, state) {
	//			return
	//		}
	//	}
	//}()
	//
	//// Create timeout context
	//ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*5)
	//defer cancel()
	//
	//// Try to wait for READY state
	//if !grpcClientConn.WaitForStateChange(ctxWithTimeout, connectivity.Idle) {
	//	return nil, fmt.Errorf("connection stuck in IDLE state")
	//}

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
