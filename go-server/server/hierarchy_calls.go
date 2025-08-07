package main

import (
	"context"
	"database/sql"
	"fmt"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
)

func (s *DataLoaderServer) GetHierarchies(request *pb.GetHierarchiesRequest, stream pb.DataLoader_GetHierarchiesServer) error {
	queryString := "SELECT * FROM public.hierarchies"
	args := []interface{}{}
	rowcount := 0

	if request.TagSetId > 0 {
		queryString += " WHERE tagset_id = $1"
		args = append(args, request.TagSetId)
	}

	rows, err := s.db.Query(queryString, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hierarchy pb.Hierarchy
		var nullableRootNodeID sql.NullInt64
		err := rows.Scan(&hierarchy.Id, &hierarchy.Name, &hierarchy.TagSetId, &nullableRootNodeID)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if nullableRootNodeID.Valid {
			hierarchy.RootNodeId = nullableRootNodeID.Int64
		}
		response := &pb.StreamingHierarchyResponse{
			Message: &pb.StreamingHierarchyResponse_Hierarchy{
				Hierarchy: &hierarchy,
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
		rowcount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	if rowcount == 0 {
		var s *status.Status
		if request.TagSetId > 0 {
			s = status.Newf(codes.NotFound, "No hierarchies found with tag set ID %d", request.TagSetId)
		} else {
			s = status.Newf(codes.NotFound, "No hierarchies found")
		}
		response := &pb.StreamingHierarchyResponse{
			Message: &pb.StreamingHierarchyResponse_Error{
				Error: s.Proto(),
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}
func (s *DataLoaderServer) GetHierarchy(ctx context.Context, request *pb.IdRequest) (*pb.Hierarchy, error) {
	row := s.db.QueryRow("SELECT * FROM public.hierarchies WHERE id = $1", request.Id)
	var hierarchy pb.Hierarchy
	var nullableRootNodeID sql.NullInt64
	err := row.Scan(&hierarchy.Id, &hierarchy.Name, &hierarchy.TagSetId, &nullableRootNodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "No hierarchy found with ID %d", request.Id)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch hierarchy from database: %s", err)
	}
	if nullableRootNodeID.Valid {
		hierarchy.RootNodeId = nullableRootNodeID.Int64
	}
	return &hierarchy, nil

}
func (s *DataLoaderServer) CreateHierarchy(ctx context.Context, request *pb.CreateHierarchyRequest) (*pb.Hierarchy, error) {
	row := s.db.QueryRow("SELECT * FROM public.hierarchies WHERE name = $1 AND tagset_id = $2", request.Name, request.TagSetId)
	var hierarchy pb.Hierarchy
	var nullableRootNodeID sql.NullInt64
	err := row.Scan(&hierarchy.Id, &hierarchy.Name, &hierarchy.TagSetId, &nullableRootNodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch hierarchy from database: %s", err)
	}
	if err == nil {
		if nullableRootNodeID.Valid {
			hierarchy.RootNodeId = nullableRootNodeID.Int64
		}
		return &hierarchy, nil
	}

	// Insert the new hierarchy into the database
	queryString := "INSERT INTO public.hierarchies (name, tagset_id) VALUES ($1, $2) RETURNING id, name, tagset_id;"
	row = s.db.QueryRow(queryString, request.Name, request.TagSetId)

	var insertedHierarchy pb.Hierarchy
	err = row.Scan(&insertedHierarchy.Id, &insertedHierarchy.Name, &insertedHierarchy.TagSetId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert hierarchy into database: %s", err)
	}

	return &insertedHierarchy, nil
}
