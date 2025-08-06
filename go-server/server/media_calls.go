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

func (s *DataLoaderServer) GetMedias(request *pb.GetMediasRequest, stream pb.DataLoader_GetMediasServer) error {
	queryString := "SELECT * FROM public.medias"
	args := []interface{}{}
	rowcount := 0

	if request.FileType > 0 {
		queryString += " WHERE file_type = $1"
		args = append(args, request.FileType)
	}

	rows, err := s.db.Query(queryString, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, fileType int64
		var fileURI, thumbnailURI string
		if err := rows.Scan(&id, &fileURI, &fileType, &thumbnailURI); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		response := &pb.StreamingMediaResponse{
			Message: &pb.StreamingMediaResponse_Media{
				Media: &pb.Media{
					Id:           id,
					FileUri:      fileURI,
					FileType:     fileType,
					ThumbnailUri: thumbnailURI,
				},
			},
		}

		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
		rowcount++
	}

	// if err := rows.Err(); err != nil {
	//     return fmt.Errorf("row iteration error: %w", err)
	// }

	if rowcount == 0 {
		var s *status.Status
		if request.FileType > 0 {
			s = status.Newf(codes.NotFound, "No results were fetched for file type %d", request.FileType)
		} else {
			s = status.Newf(codes.NotFound, "No results were fetched")
		}
		response := &pb.StreamingMediaResponse{
			Message: &pb.StreamingMediaResponse_Error{
				Error: s.Proto(),
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}

	return nil
}

func (s *DataLoaderServer) GetMediaById(ctx context.Context, request *pb.IdRequest) (*pb.Media, error) {
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE id = $1", request.Id)

	var media pb.Media
	err := row.Scan(&media.Id, &media.FileUri, &media.FileType, &media.ThumbnailUri)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "No media found with ID %d", request.Id)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch media from database: %s", err)
	}

	return &media, nil
}

func (s *DataLoaderServer) GetMediaByURI(ctx context.Context, request *pb.GetMediaByURIRequest) (*pb.Media, error) {
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE file_uri = $1", request.FileUri)

	var media pb.Media
	err := row.Scan(&media.Id, &media.FileUri, &media.FileType, &media.ThumbnailUri)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "No media found with URI %s", request.FileUri)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch media from database: %s", err)
	}

	return &media, nil
}

func (s *DataLoaderServer) CreateMedia(ctx context.Context, request *pb.Media) (*pb.Media, error) {
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE file_uri = $1", request.FileUri)

	var existingMedia pb.Media
	err := row.Scan(&existingMedia.Id, &existingMedia.FileUri, &existingMedia.FileType, &existingMedia.ThumbnailUri)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch media from database: %s", err)
	}

	if err == nil {
		if existingMedia.FileType == request.FileType && existingMedia.ThumbnailUri == request.ThumbnailUri {
			return &existingMedia, nil
		}

		return nil, status.Errorf(codes.AlreadyExists, "Media URI '%s' already exists with a different type or thumbnail_uri", request.FileUri)
	}

	// Insert the new media into the database
	queryString := "INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES ($1, $2, $3) RETURNING *;"
	var insertedMedia pb.Media

	row = s.db.QueryRow(queryString, request.FileUri, request.FileType, request.ThumbnailUri)
	err = row.Scan(&insertedMedia.Id, &insertedMedia.FileUri, &insertedMedia.FileType, &insertedMedia.ThumbnailUri)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert media into database: %s", err)
	}

	return &insertedMedia, nil
}

func (s *DataLoaderServer) CreateMedias(stream pb.DataLoader_CreateMediaStreamServer) error {
	requestCounter, dataCounter := 0, 1
	var queryString string
	var data []interface{}

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read request: %w", err)
		}

		requestCounter++
		if requestCounter%BATCH_SIZE == 1 {
			queryString = "INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES "
			data = []interface{}{}
		}

		queryString += fmt.Sprintf("($%d, $%d, $%d),", dataCounter, dataCounter+1, dataCounter+2)
		data = append(data,
			request.FileUri,
			request.FileType,
			request.ThumbnailUri,
		)
		dataCounter += 3

		if requestCounter%BATCH_SIZE == 0 {
			dataCounter = 1
			queryString = queryString[:len(queryString)-1] + ";"
			_, err := s.db.Exec(queryString, data...)
			if err != nil {
				log.Printf("Error: %s", err)
				err = stream.Send(&pb.CreateMediaStreamResponse{
					Message: &pb.CreateMediaStreamResponse_Error{
						Error: status.Newf(codes.Internal, err.Error()).Proto(),
					},
				})
				if err != nil {
					return fmt.Errorf("failed to send response: %w", err)
				}
			} else {
				err = stream.Send(&pb.CreateMediaStreamResponse{
					Message: &pb.CreateMediaStreamResponse_Count{
						Count: int64(requestCounter),
					},
				})
				if err != nil {
					return fmt.Errorf("failed to send response: %w", err)
				}
			}
		}
	}

	if requestCounter%BATCH_SIZE > 0 {
		queryString = queryString[:len(queryString)-1] + ";"
		_, err := s.db.Exec(queryString, data...)
		if err != nil {
			log.Printf("Error: %s", err)
			err = stream.Send(&pb.CreateMediaStreamResponse{
				Message: &pb.CreateMediaStreamResponse_Error{
					Error: status.Newf(codes.Internal, err.Error()).Proto(),
				},
			})
			if err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
		} else {
			err = stream.Send(&pb.CreateMediaStreamResponse{
				Message: &pb.CreateMediaStreamResponse_Count{
					Count: int64(requestCounter),
				},
			})
			if err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
		}
	}
	return nil
}

func (s *DataLoaderServer) DeleteMedia(ctx context.Context, request *pb.IdRequest) (*pb.Empty, error) {
	queryString := "DELETE FROM public.medias WHERE id=$1;"

	result, err := s.db.Exec(queryString, request.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to execute delete query: %s", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get rows affected: %s", err)
	}

	if rowsAffected > 0 {
		response := &pb.Empty{}
		return response, nil
	} else {
		return nil, status.Errorf(codes.NotFound, "No media found with ID %d", request.Id)
	}
}
