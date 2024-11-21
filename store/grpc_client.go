package store

import (
	"genesis/proto"

	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
)

func StartGRPCClient(destNodeAddr string) (proto.DataMigrationServiceClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(destNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("gRPC client started on port ", destNodeAddr)
	client := proto.NewDataMigrationServiceClient(conn)

	return client, conn
}
