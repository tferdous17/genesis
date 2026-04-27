package store

import (
	"github.com/tferdous17/genesis/proto"

	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func StartGRPCClient(destNodeAddr string) (proto.DataMigrationServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(destNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("create gRPC client to %s: %w", destNodeAddr, err)
	}
	fmt.Println("gRPC client started on port ", destNodeAddr)
	client := proto.NewDataMigrationServiceClient(conn)

	return client, conn, nil
}
