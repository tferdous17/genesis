package store

import (
	"bitcask-go/proto"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"net"
)

type dataMigrationServer struct {
	proto.UnimplementedDataMigrationServiceServer

	underlyingNode *Node
}

func (d *dataMigrationServer) MigrateKeyValuePairs(ctx context.Context, req *proto.KeyValueMigrationRequest) (*proto.KeyValueMigrationResponse, error) {
	fmt.Println(req)
	var migrationResults []*proto.MigrationResult

	for i := range req.KvPairs {
		fmt.Println("storing data into node at address ", d.underlyingNode.Addr)
		d.underlyingNode.Store.PutRecordFromGRPC(req.KvPairs[i].Record)

		res := proto.MigrationResult{
			Key:      req.KvPairs[i].Record.Key,
			Success:  true,
			ErrorMsg: "",
		}
		migrationResults = append(migrationResults, &res)
	}

	return &proto.KeyValueMigrationResponse{
		Success:          true,
		ErrorMsg:         "",
		MigrationResults: migrationResults,
	}, nil
}

func StartGRPCServer(addr string, node *Node) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
	}

	server := grpc.NewServer()
	service := &dataMigrationServer{underlyingNode: node}
	proto.RegisterDataMigrationServiceServer(server, service)

	go func() {
		fmt.Println("gRPC server started @ port ", addr)
		err = server.Serve(ln)
		if err != nil {
			fmt.Println(err)
		}
	}()
}