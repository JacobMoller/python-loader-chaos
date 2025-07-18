package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
)

// !================================= Taggings
func (s *DataLoaderServer) GetTaggings(request *pb.Empty, stream pb.DataLoader_GetTaggingsServer) error {
	queryString := "SELECT * FROM public.taggings"
	rowcount := 0
	rows, err := s.db.Query(queryString)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var media_id, tag_id int64
		if err := rows.Scan(&media_id, &tag_id); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		response := &pb.StreamingTaggingResponse{
			Message: &pb.StreamingTaggingResponse_Tagging{
				Tagging: &pb.Tagging{
					MediaId: media_id,
					TagId:   tag_id,
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
		response := &pb.StreamingTaggingResponse{
			Message: &pb.StreamingTaggingResponse_Error{
				Error: status.Newf(codes.NotFound, "No taggings found").Proto(),
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}

func (s *DataLoaderServer) GetMediasWithTag(ctx context.Context, request *pb.IdRequest) (*pb.RepeatedIdResponse, error) {
	queryString := "SELECT object_id FROM public.taggings WHERE tag_id = $1"
	rows, err := s.db.Query(queryString, request.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to execute query: %s", err)
	}
	defer rows.Close()

	var media_ids []int64
	for rows.Next() {
		var media_id int64
		if err := rows.Scan(&media_id); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to scan: %s", err)
		}
		media_ids = append(media_ids, media_id)
	}

	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "Row iteration error: %s", err)
	}

	if len(media_ids) == 0 {
		return nil, status.Errorf(codes.NotFound, "No media found with tag ID %d", request.Id)
	}
	return &pb.RepeatedIdResponse{Ids: media_ids}, nil
}

func (s *DataLoaderServer) GetMediaTags(ctx context.Context, request *pb.IdRequest) (*pb.RepeatedIdResponse, error) {
	queryString := "SELECT tag_id FROM public.taggings WHERE object_id = $1"
	rows, err := s.db.Query(queryString, request.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to execute query: %s", err)
	}
	defer rows.Close()

	var tag_ids []int64
	for rows.Next() {
		var tag_id int64
		if err := rows.Scan(&tag_id); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to scan: %s", err)
		}
		tag_ids = append(tag_ids, tag_id)
	}

	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "Row iteration error: %s", err)
	}

	if len(tag_ids) == 0 {
		return nil, status.Errorf(codes.NotFound, "No tags found for media ID %d", request.Id)
	}
	return &pb.RepeatedIdResponse{Ids: tag_ids}, nil
}
func (s *DataLoaderServer) CreateTagging(ctx context.Context, request *pb.CreateTaggingRequest) (*pb.Tagging, error) {
	row := s.db.QueryRow("SELECT * FROM public.taggings WHERE object_id = $1 AND tag_id = $2", request.MediaId, request.TagId)
	var existingTagging pb.Tagging
	err := row.Scan(&existingTagging.MediaId, &existingTagging.TagId)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch tagging from database: %s", err)
	}
	// Tagging already exists
	if err == nil {
		return &existingTagging, nil
	}

	// Insert the new media into the database
	queryString := "INSERT INTO public.taggings (object_id, tag_id) VALUES ($1, $2) RETURNING *;"
	row = s.db.QueryRow(queryString, request.MediaId, request.TagId)

	var insertedTagging pb.Tagging
	err = row.Scan(&insertedTagging.MediaId, &insertedTagging.TagId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert tagging into database: %s", err)
	}

	return &insertedTagging, nil
}

func (s *DataLoaderServer) CreateTaggingStream(stream pb.DataLoader_CreateTaggingStreamServer) error {
	// log.Print("Recived Create Tagging Stream Request")
	requestCounter, dataCounter := 0, 1
	var queryString string
	var data []interface{}

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("failed to read request: %s", err.Error())
			return fmt.Errorf("failed to read request: %w", err)
		}

		requestCounter++
		if requestCounter%BATCH_SIZE == 1 {
			queryString = "INSERT INTO public.taggings (object_id, tag_id) VALUES "
			data = []interface{}{}
		}

		queryString += fmt.Sprintf("($%d, $%d),", dataCounter, dataCounter+1)
		data = append(data,
			request.MediaId,
			request.TagId,
		)
		dataCounter += 2

		if requestCounter%BATCH_SIZE == 0 {
			dataCounter = 1
			queryString = queryString[:len(queryString)-1] + ";"
			res, err := s.db.Exec(queryString, data...)
			if err != nil {
				log.Printf("Error: %s", err.Error())
				if err = stream.Send(&pb.CreateTaggingStreamResponse{
					Message: &pb.CreateTaggingStreamResponse_Error{
						Error: status.Newf(codes.Internal, "%s", err.Error()).Proto(),
					},
				}); err != nil {
					return fmt.Errorf("failed to send response: %w", err)
				}
				continue
			}
			rowsAffected, _ := res.RowsAffected()
			if err = stream.Send(&pb.CreateTaggingStreamResponse{
				Message: &pb.CreateTaggingStreamResponse_Count{
					Count: int64(rowsAffected),
				},
			}); err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
		}
	}

	if requestCounter%BATCH_SIZE > 0 {
		queryString = queryString[:len(queryString)-1] + ";"
		res, err := s.db.Exec(queryString, data...)
		if err != nil {
			log.Printf("Error: %s", err)
			if err = stream.Send(&pb.CreateTaggingStreamResponse{
				Message: &pb.CreateTaggingStreamResponse_Error{
					Error: status.Newf(codes.Internal, "%s", err.Error()).Proto(),
				},
			}); err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
			return nil
		}
		rowsAffected, _ := res.RowsAffected()
		if err = stream.Send(&pb.CreateTaggingStreamResponse{
			Message: &pb.CreateTaggingStreamResponse_Count{
				Count: int64(rowsAffected),
			},
		}); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	// log.Println("Request correctly terminated")
	return nil
}
