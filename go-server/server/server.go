package main

import (
	"context"
	"database/sql"
	"fmt"

	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	_ "github.com/lib/pq"

	pb "m3.dataloader/dataloader"
)

const (
	dbname     = "loader-testing"
	user       = "postgres"
	pwd        = "root"
	db_host    = "localhost"
	db_port    = 5432
	sv_host    = "localhost"
	sv_port    = 50051
	BATCH_SIZE = 5000
)

type DataLoaderServer struct {
	pb.UnimplementedDataLoaderServer
	db *sql.DB
}

func NewDataLoaderServer(connStr string) (*DataLoaderServer, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %w", err)
	}
	fmt.Println("new data loader server created")

	// Ensure the database connection is valid
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping the database: %w", err)
	}

	return &DataLoaderServer{
		db: db,
	}, nil
}

// Close closes the database connection.
func (s *DataLoaderServer) Close() {
	s.db.Close()
}

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

func main() {
	conn_str := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%d sslmode=disable",
		dbname,  // dbname
		user,    // user
		pwd,     // password
		db_host, // host
		db_port) // port

	server, err := NewDataLoaderServer(conn_str)
	if err != nil {
		log.Fatalf("Error creating server: %v", err)
	}
	defer server.Close()

	// Create a TCP listener for the gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", sv_host, sv_port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create and register the implementation of the gRPC server
	grpc_server := grpc.NewServer()
	pb.RegisterDataLoaderServer(grpc_server, server)
	log.Println("gRPC server listening on port 50051")
	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
