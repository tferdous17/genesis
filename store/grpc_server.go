package store

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/tferdous17/genesis/proto"
)

type dataMigrationServer struct {
	proto.UnimplementedDataMigrationServiceServer

	underlyingNode *Node
}

func (d *dataMigrationServer) MigrateKeyValuePairs(ctx context.Context, req *proto.KeyValueMigrationRequest) (*proto.KeyValueMigrationResponse, error) {
	fmt.Println(req)
	migrationResults := make([]*proto.MigrationResult, 0, len(req.KvPairs))
	overallSuccess := true

	for _, kv := range req.KvPairs {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("migration cancelled: %w", err)
		}

		result := &proto.MigrationResult{Key: kv.Record.Key}

		if err := d.underlyingNode.Store.PutRecordFromGRPC(kv.Record); err != nil {
			// record the failure but continue -- caller needs to know which pairs succeeded
			result.Success = false
			result.ErrorMsg = err.Error()
			overallSuccess = false
		} else {
			result.Success = true
		}

		migrationResults = append(migrationResults, result)
	}

	return &proto.KeyValueMigrationResponse{
		Success:          overallSuccess,
		MigrationResults: migrationResults,
	}, nil
}

func StartGRPCServer(addr string, node *Node) (*grpc.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	server := grpc.NewServer()
	service := &dataMigrationServer{underlyingNode: node}
	proto.RegisterDataMigrationServiceServer(server, service)

	go func() {
		log.Printf("gRPC server listening on %s", addr)
		err = server.Serve(ln)
		if err != nil {
			log.Printf("failed to start gRPC server: %v", err)
		}
	}()
	return server, nil
}
