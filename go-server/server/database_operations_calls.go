package main

import (
	"context"
	"fmt"
	"os"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
)

func (s *DataLoaderServer) ResetDatabase(ctx context.Context, request *pb.Empty) (*pb.Empty, error) {
	ddlSQL, err := os.ReadFile("../../ddl.sql")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to read DDL file: %s", err)
	}

	_, err = s.db.Exec(string(ddlSQL))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to execute DDL: %s", err)
	}

	fmt.Println("DB has been reset")
	response := &pb.Empty{}
	return response, nil
}
