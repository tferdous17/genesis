package store

import (
	"github.com/tferdous17/genesis/proto"

	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
