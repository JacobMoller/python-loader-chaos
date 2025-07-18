package main

import (
	"context"
	"database/sql"
	"fmt"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
)

// !================================= Tagsets
func (s *DataLoaderServer) GetTagSets(request *pb.GetTagSetsRequest, stream pb.DataLoader_GetTagSetsServer) error {
	queryString := "SELECT * FROM public.tagsets"
	args := []interface{}{}
	rowcount := 0

	if request.TagTypeId > 0 {
		queryString += " WHERE tagtype_id = $1"
		args = append(args, request.TagTypeId)
	}

	rows, err := s.db.Query(queryString, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, tagtype_id int64
		var name string
		if err := rows.Scan(&id, &name, &tagtype_id); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		response := &pb.StreamingTagSetResponse{
			Message: &pb.StreamingTagSetResponse_Tagset{
				Tagset: &pb.TagSet{
					Id:        id,
					Name:      name,
					TagTypeId: tagtype_id,
				},
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
		if request.TagTypeId > 0 {
			s = status.Newf(codes.NotFound, "No tagsets found with tag type ID %d", request.TagTypeId)
		} else {
			s = status.Newf(codes.NotFound, "No tagsets found")
		}
		response := &pb.StreamingTagSetResponse{
			Message: &pb.StreamingTagSetResponse_Error{
				Error: s.Proto(),
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}
func (s *DataLoaderServer) GetTagSetById(ctx context.Context, request *pb.IdRequest) (*pb.TagSet, error) {
	row := s.db.QueryRow("SELECT * FROM public.tagsets WHERE id = $1", request.Id)

	var tagset pb.TagSet
	err := row.Scan(&tagset.Id, &tagset.Name, &tagset.TagTypeId)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "No tagset found with ID %d", request.Id)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch tagset from database: %s", err)
	}

	return &tagset, nil
}

func (s *DataLoaderServer) GetTagSetByName(ctx context.Context, request *pb.GetTagSetRequestByName) (*pb.TagSet, error) {
	row := s.db.QueryRow("SELECT * FROM public.tagsets WHERE name = $1", request.Name)

	var tagset pb.TagSet
	err := row.Scan(&tagset.Id, &tagset.Name, &tagset.TagTypeId)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "No tagset found with name %s", request.Name)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch tagset from database: %s", err)
	}

	return &tagset, nil
}
func (s *DataLoaderServer) CreateTagSet(ctx context.Context, request *pb.CreateTagSetRequest) (*pb.TagSet, error) {
	row := s.db.QueryRow("SELECT * FROM public.tagsets WHERE name = $1", request.Name)

	var existingTagset pb.TagSet
	err := row.Scan(&existingTagset.Id, &existingTagset.Name, &existingTagset.TagTypeId)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch tagset from database: %s", err)
	}

	if err == nil {
		if existingTagset.TagTypeId == request.TagTypeId {
			return &existingTagset, nil
		}

		return nil, status.Errorf(codes.AlreadyExists, "Tagset name '%s' already exists with a different type", request.Name)
	}

	// Insert the new media into the database
	queryString := "INSERT INTO public.tagsets (name, tagtype_id) VALUES ($1, $2) RETURNING *;"
	row = s.db.QueryRow(queryString, request.Name, request.TagTypeId)

	var insertedTagset pb.TagSet
	err = row.Scan(&insertedTagset.Id, &insertedTagset.Name, &insertedTagset.TagTypeId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert tagset into database: %s", err)
	}

	return &insertedTagset, nil
}
