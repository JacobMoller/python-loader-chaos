package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"

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

// !================================= Medias
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

		response := &pb.MediaResponse{
			Media: &pb.Media{
				Id:           id,
				FileUri:      fileURI,
				FileType:     fileType,
				ThumbnailUri: thumbnailURI,
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
		response := &pb.MediaResponse{ErrorMessage: "No results were fetched"}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}

	return nil
}

func (s *DataLoaderServer) GetMediaById(ctx context.Context, request *pb.IdRequest) (*pb.MediaResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE id = $1", request.Id)

	var media pb.Media
	err := row.Scan(&media.Id, &media.FileUri, &media.FileType, &media.ThumbnailUri)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.MediaResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.MediaResponse{ErrorMessage: fmt.Sprintf("Failed to fetch media from database: %s", err)}, nil
	}

	return &pb.MediaResponse{
		Media: &media,
	}, nil
}

func (s *DataLoaderServer) GetMediaByURI(ctx context.Context, request *pb.GetMediaByURIRequest) (*pb.MediaResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE file_uri = $1", request.FileUri)

	var media pb.Media
	err := row.Scan(&media.Id, &media.FileUri, &media.FileType, &media.ThumbnailUri)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.MediaResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.MediaResponse{ErrorMessage: fmt.Sprintf("Failed to fetch media from database: %s", err)}, nil
	}

	return &pb.MediaResponse{
		Media: &media,
	}, nil
}

func (s *DataLoaderServer) CreateMedia(ctx context.Context, request *pb.CreateMediaRequest) (*pb.MediaResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE file_uri = $1", request.Media.FileUri)

	var existingMedia pb.Media
	err := row.Scan(&existingMedia.Id, &existingMedia.FileUri, &existingMedia.FileType, &existingMedia.ThumbnailUri)
	if err != nil && err != sql.ErrNoRows {
		return &pb.MediaResponse{ErrorMessage: fmt.Sprintf("Failed to fetch media from database: %s", err)}, nil
	}

	if err == nil {
		if existingMedia.FileType == request.Media.FileType && existingMedia.ThumbnailUri == request.Media.ThumbnailUri {
			return &pb.MediaResponse{
				Media: &existingMedia,
			}, nil
		}

		return &pb.MediaResponse{
			ErrorMessage: fmt.Sprintf("Error: Media URI '%s' already exists with a different type or thumbnail_uri", request.Media.FileUri),
		}, nil
	}

	// Insert the new media into the database
	queryString := "INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES ($1, $2, $3) RETURNING *;"
	var insertedMedia pb.Media

	row = s.db.QueryRow(queryString, request.Media.FileUri, request.Media.FileType, request.Media.ThumbnailUri)
	err = row.Scan(&insertedMedia.Id, &insertedMedia.FileUri, &insertedMedia.FileType, &insertedMedia.ThumbnailUri)
	if err != nil {
		return &pb.MediaResponse{ErrorMessage: fmt.Sprintf("Failed to insert media into database: %s", err)}, nil
	}

	return &pb.MediaResponse{
		Media: &insertedMedia,
	}, nil
}

func (s *DataLoaderServer) CreateMedias(stream pb.DataLoader_CreateMediasServer) error {
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
			request.Media.FileUri,
			request.Media.FileType,
			request.Media.ThumbnailUri,
		)
		dataCounter += 3

		if requestCounter%BATCH_SIZE == 0 {
			dataCounter = 1
			queryString = queryString[:len(queryString)-1] + ";"
			_, err := s.db.Exec(queryString, data...)
			if err != nil {
				log.Printf("Error: %s", err)
				err = stream.Send(&pb.CreateMediaStreamResponse{
					ErrorMessage: fmt.Sprintf("Error: %s", err),
				})
				if err != nil {
					return fmt.Errorf("failed to send response: %w", err)
				}
			} else {
				err = stream.Send(&pb.CreateMediaStreamResponse{
					Count: int64(requestCounter),
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
				ErrorMessage: fmt.Sprintf("Error: %s", err),
			})
			if err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
		} else {
			err = stream.Send(&pb.CreateMediaStreamResponse{
				Count: int64(requestCounter),
			})
			if err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
		}
	}
	return nil
}

func (s *DataLoaderServer) DeleteMedia(ctx context.Context, request *pb.IdRequest) (*pb.StatusResponse, error) {
	queryString := "DELETE FROM public.medias WHERE id=$1;"

	result, err := s.db.Exec(queryString, request.Id)
	if err != nil {
		return &pb.StatusResponse{ErrorMessage: fmt.Sprintf("Failed to execute delete query: %s", err)}, nil
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &pb.StatusResponse{ErrorMessage: fmt.Sprintf("Failed to get rows affected: %s", err.Error())}, nil
	}

	if rowsAffected > 0 {
		response := &pb.StatusResponse{}
		return response, nil
	} else {
		return &pb.StatusResponse{ErrorMessage: "Element not found"}, nil
	}
}

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

		response := &pb.TagSetResponse{
			Tagset: &pb.TagSet{
				Id:        id,
				Name:      name,
				TagTypeId: tagtype_id,
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
		response := &pb.TagSetResponse{ErrorMessage: "No results were fetched"}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}
func (s *DataLoaderServer) GetTagSetById(ctx context.Context, request *pb.IdRequest) (*pb.TagSetResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.tagsets WHERE id = $1", request.Id)

	var tagset pb.TagSet
	err := row.Scan(&tagset.Id, &tagset.Name, &tagset.TagTypeId)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.TagSetResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.TagSetResponse{ErrorMessage: fmt.Sprintf("Failed to fetch tagset from database: %s", err)}, nil
	}

	return &pb.TagSetResponse{
		Tagset: &tagset,
	}, nil
}

func (s *DataLoaderServer) GetTagSetByName(ctx context.Context, request *pb.GetTagSetRequestByName) (*pb.TagSetResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.tagsets WHERE name = $1", request.Name)

	var tagset pb.TagSet
	err := row.Scan(&tagset.Id, &tagset.Name, &tagset.TagTypeId)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.TagSetResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.TagSetResponse{ErrorMessage: fmt.Sprintf("Failed to fetch tagset from database: %s", err)}, nil
	}

	return &pb.TagSetResponse{
		Tagset: &tagset,
	}, nil
}
func (s *DataLoaderServer) CreateTagSet(ctx context.Context, request *pb.CreateTagSetRequest) (*pb.TagSetResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.tagsets WHERE name = $1", request.Name)

	var existingTagset pb.TagSet
	err := row.Scan(&existingTagset.Id, &existingTagset.Name, &existingTagset.TagTypeId)
	if err != nil && err != sql.ErrNoRows {
		return &pb.TagSetResponse{ErrorMessage: fmt.Sprintf("Failed to fetch tagset from database: %s", err)}, nil
	}

	if err == nil {
		if existingTagset.TagTypeId == request.TagTypeId {
			return &pb.TagSetResponse{
				Tagset: &existingTagset,
			}, nil
		}

		return &pb.TagSetResponse{
			ErrorMessage: fmt.Sprintf("Error: Tagset name '%s' already exists with a different type", request.Name),
		}, nil
	}

	// Insert the new media into the database
	queryString := "INSERT INTO public.tagsets (name, tagtype_id) VALUES ($1, $2) RETURNING *;"
	row = s.db.QueryRow(queryString, request.Name, request.TagTypeId)

	var insertedTagset pb.TagSet
	err = row.Scan(&insertedTagset.Id, &insertedTagset.Name, &insertedTagset.TagTypeId)
	if err != nil {
		return &pb.TagSetResponse{ErrorMessage: fmt.Sprintf("Failed to insert tagset into database: %s", err)}, nil
	}

	return &pb.TagSetResponse{
		Tagset: &insertedTagset,
	}, nil
}

// !================================= Tags
func (s *DataLoaderServer) GetTags(request *pb.GetTagsRequest, stream pb.DataLoader_GetTagsServer) error {
	queryString := `SELECT
    t.id,
    t.tagtype_id,
    t.tagset_id,
    ant.name as text_value,
	tst.name::text as timestamp_value,
	tt.name::text as time_value,
	dt.name::text as date_value,
	nt.name as num_value
FROM
    public.tags t
LEFT JOIN
    public.alphanumerical_tags ant ON t.id = ant.id
LEFT JOIN
    public.timestamp_tags tst ON t.id = tst.id
LEFT JOIN
    public.time_tags tt ON t.id = tt.id
LEFT JOIN
    public.date_tags dt ON t.id = dt.id
LEFT JOIN
    public.numerical_tags nt ON t.id = nt.id`

	rowcount := 0

	if request.TagTypeId > 0 || request.TagSetId > 0 {
		queryString += " WHERE "
		if request.TagTypeId > 0 && request.TagSetId > 0 {
			queryString += fmt.Sprintf("t.tagset_id = %d AND t.tagtype_id = %d", request.TagSetId, request.TagTypeId)
		} else if request.TagSetId > 0 {
			queryString += fmt.Sprintf("t.tagset_id = %d", request.TagSetId)
		} else {
			queryString += fmt.Sprintf("t.tagtype_id = %d", request.TagTypeId)
		}
	}

	rows, err := s.db.Query(queryString)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, tagset_id, tagtype_id int64
		var num_value sql.NullInt64
		var text_value, timestamp_value, time_value, date_value sql.NullString
		if err := rows.Scan(&id, &tagtype_id, &tagset_id, &text_value, &timestamp_value, &time_value, &date_value, &num_value); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		response := &pb.TagResponse{}
		switch tagtype_id {
		case 1:
			if text_value.Valid {
				response = &pb.TagResponse{
					Tag: &pb.Tag{
						Id:        id,
						TagSetId:  tagset_id,
						TagTypeId: tagtype_id,
						Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: text_value.String}},
					},
				}
			} else {
				response = &pb.TagResponse{ErrorMessage: fmt.Sprintf("Error: Null value for tag id=%d", id)}
			}
		case 2:
			if timestamp_value.Valid {
				response = &pb.TagResponse{
					Tag: &pb.Tag{
						Id:        id,
						TagSetId:  tagset_id,
						TagTypeId: tagtype_id,
						Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: timestamp_value.String}},
					},
				}
			} else {
				response = &pb.TagResponse{ErrorMessage: fmt.Sprintf("Error: Null value for tag id=%d", id)}
			}
		case 3:
			if time_value.Valid {
				response = &pb.TagResponse{
					Tag: &pb.Tag{
						Id:        id,
						TagSetId:  tagset_id,
						TagTypeId: tagtype_id,
						Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: time_value.String}},
					},
				}
			} else {
				response = &pb.TagResponse{ErrorMessage: fmt.Sprintf("Error: Null value for tag id=%d", id)}
			}
		case 4:
			if date_value.Valid {
				response = &pb.TagResponse{
					Tag: &pb.Tag{
						Id:        id,
						TagSetId:  tagset_id,
						TagTypeId: tagtype_id,
						Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: date_value.String}},
					},
				}
			} else {
				response = &pb.TagResponse{ErrorMessage: fmt.Sprintf("Error: Null value for tag id=%d", id)}
			}
		case 5:
			if num_value.Valid {
				response = &pb.TagResponse{
					Tag: &pb.Tag{
						Id:        id,
						TagSetId:  tagset_id,
						TagTypeId: tagtype_id,
						Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: num_value.Int64}},
					},
				}
			} else {
				response = &pb.TagResponse{ErrorMessage: fmt.Sprintf("Error: Null value for tag id=%d", id)}
			}
		default:
			response = &pb.TagResponse{ErrorMessage: fmt.Sprintf("Error: Incorrect type for tag id=%d", id)}
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
		response := &pb.TagResponse{ErrorMessage: "No results were fetched"}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}

func (s *DataLoaderServer) GetTag(ctx context.Context, request *pb.IdRequest) (*pb.TagResponse, error) {
	queryString := `SELECT
    t.id,
    t.tagtype_id,
    t.tagset_id,
    COALESCE(ant.name, tst.name::text, tt.name::text, dt.name::text, nt.name::text) as value
FROM
    (SELECT * FROM public.tags WHERE id = $1) t
LEFT JOIN
    public.alphanumerical_tags ant ON t.id = ant.id
LEFT JOIN
    public.timestamp_tags tst ON t.id = tst.id
LEFT JOIN
    public.time_tags tt ON t.id = tt.id
LEFT JOIN
    public.date_tags dt ON t.id = dt.id
LEFT JOIN
    public.numerical_tags nt ON t.id = nt.id`

	row := s.db.QueryRow(queryString, request.Id)
	var tag_id, tagtype_id, tagset_id int
	var value string
	err := row.Scan(&tag_id, &tagtype_id, &tagset_id, &value)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.TagResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to fetch tag from database: %s", err)}, nil
	}

	switch tagtype_id {
	case 1:
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: value}},
			},
		}, nil
	case 2:
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: value}},
			},
		}, nil
	case 3:
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: value}},
			},
		}, nil
	case 4:
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: value}},
			},
		}, nil
	case 5:
		num_value, err := strconv.Atoi(value)
		if err != nil {
			return &pb.TagResponse{ErrorMessage: "Error converting tag value to integer."}, nil
		}
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: int64(num_value)}},
			},
		}, nil
	}
	return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Invalid tag type was fetched: %s\nCheck database integrity.", err)}, nil
}

func (s *DataLoaderServer) CreateTag(ctx context.Context, request *pb.CreateTagRequest) (*pb.TagResponse, error) {
	queryString := `SELECT t.id, t.tagtype_id, t.tagset_id, a.name::text as value 
FROM 
    (SELECT * FROM public.tags WHERE tagset_id = $1 AND tagtype_id = $2) t
    LEFT JOIN `
	data := []interface{}{}
	data = append(data, request.TagSetId, request.TagTypeId)

	switch request.TagTypeId {
	case 1:
		queryString += "public.alphanumerical_tags a ON t.id = a.id WHERE a.name = $3"
		data = append(data, request.GetAlphanumerical().Value)
	case 2:
		queryString += "public.timestamp_tags a ON t.id = a.id WHERE a.name = $3"
		data = append(data, request.GetTimestamp().Value)
	case 3:
		queryString += "public.time_tags a ON t.id = a.id WHERE a.name = $3"
		data = append(data, request.GetTime().Value)
	case 4:
		queryString += "public.date_tags a ON t.id = a.id WHERE a.name = $3"
		data = append(data, request.GetDate().Value)
	case 5:
		queryString += "public.numerical_tags a ON t.id = a.id WHERE a.name = $3"
		data = append(data, request.GetNumerical().Value)
	default:
		return &pb.TagResponse{ErrorMessage: "invalid tag type: range is 1-5"}, nil
	}

	row := s.db.QueryRow(queryString, data...)
	var tag_id, tagtype_id, tagset_id int
	var value string
	err := row.Scan(&tag_id, &tagtype_id, &tagset_id, &value)
	if err != nil && err != sql.ErrNoRows {
		return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to fetch tag from database: %s", err)}, nil
	}
	if err == nil {
		switch tagtype_id {
		case 1:
			return &pb.TagResponse{
				Tag: &pb.Tag{
					Id:        int64(tag_id),
					TagSetId:  int64(tagset_id),
					TagTypeId: int64(tagtype_id),
					Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: value}},
				},
			}, nil
		case 2:
			return &pb.TagResponse{
				Tag: &pb.Tag{
					Id:        int64(tag_id),
					TagSetId:  int64(tagset_id),
					TagTypeId: int64(tagtype_id),
					Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: value}},
				},
			}, nil
		case 3:
			return &pb.TagResponse{
				Tag: &pb.Tag{
					Id:        int64(tag_id),
					TagSetId:  int64(tagset_id),
					TagTypeId: int64(tagtype_id),
					Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: value}},
				},
			}, nil
		case 4:
			return &pb.TagResponse{
				Tag: &pb.Tag{
					Id:        int64(tag_id),
					TagSetId:  int64(tagset_id),
					TagTypeId: int64(tagtype_id),
					Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: value}},
				},
			}, nil
		case 5:
			num_value, err := strconv.Atoi(value)
			if err != nil {
				return &pb.TagResponse{ErrorMessage: "Error converting tag value to integer."}, nil
			}
			return &pb.TagResponse{
				Tag: &pb.Tag{
					Id:        int64(tag_id),
					TagSetId:  int64(tagset_id),
					TagTypeId: int64(tagtype_id),
					Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: int64(num_value)}},
				},
			}, nil
		}
	}

	// If non existent and no type issues, create the new tag
	var insertedId int64
	queryString = "INSERT INTO public.tags (tagtype_id, tagset_id) VALUES ($1, $2) RETURNING id"
	row = s.db.QueryRow(queryString, request.TagTypeId, request.TagSetId)
	err = row.Scan(&insertedId)
	if err != nil {
		return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to insert tag into database: %s", err)}, nil
	}

	queryString = "INSERT INTO public."
	switch request.TagTypeId {
	case 1:
		var value string
		queryString += "alphanumerical_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name"
		row = s.db.QueryRow(queryString, insertedId, request.GetAlphanumerical().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to insert tag into database: %s", err)}, nil
		}
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        insertedId,
				TagSetId:  request.TagSetId,
				TagTypeId: request.TagTypeId,
				Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: value}},
			},
		}, nil
	case 2:
		var value string
		queryString += "timestamp_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name::text"
		row = s.db.QueryRow(queryString, insertedId, request.GetTimestamp().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to insert tag into database: %s", err)}, nil
		}
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        insertedId,
				TagSetId:  request.TagSetId,
				TagTypeId: request.TagTypeId,
				Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: value}},
			},
		}, nil
	case 3:
		var value string
		queryString += "time_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name::text"
		row = s.db.QueryRow(queryString, insertedId, request.GetTime().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to insert tag into database: %s", err)}, nil
		}
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        insertedId,
				TagSetId:  request.TagSetId,
				TagTypeId: request.TagTypeId,
				Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: value}},
			},
		}, nil
	case 4:
		var value string
		queryString += "date_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name::text"
		row = s.db.QueryRow(queryString, insertedId, request.GetDate().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to insert tag into database: %s", err)}, nil
		}
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        insertedId,
				TagSetId:  request.TagSetId,
				TagTypeId: request.TagTypeId,
				Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: value}},
			},
		}, nil
	case 5:
		var value int64
		queryString += "numerical_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name"
		row = s.db.QueryRow(queryString, insertedId, request.GetNumerical().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return &pb.TagResponse{ErrorMessage: fmt.Sprintf("Failed to insert tag into database: %s", err)}, nil
		}
		return &pb.TagResponse{
			Tag: &pb.Tag{
				Id:        insertedId,
				TagSetId:  request.TagSetId,
				TagTypeId: request.TagTypeId,
				Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: value}},
			},
		}, nil
	default:
		return &pb.TagResponse{ErrorMessage: "Error: incorrect tag type was provided."}, nil
	}
}

// !================================= Taggings
func (s *DataLoaderServer) GetTaggings(request *pb.EmptyRequest, stream pb.DataLoader_GetTaggingsServer) error {
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

		response := &pb.TaggingResponse{
			Tagging: &pb.Tagging{
				MediaId: media_id,
				TagId:   tag_id,
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
		response := &pb.TaggingResponse{ErrorMessage: "No results were fetched"}
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
		return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("Failed to execute query: %s", err)}, nil
	}
	defer rows.Close()

	var media_ids []int64
	for rows.Next() {
		var media_id int64
		if err := rows.Scan(&media_id); err != nil {
			return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("Failed to scan: %s", err)}, nil
		}
		media_ids = append(media_ids, media_id)
	}

	if err := rows.Err(); err != nil {
		return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("Row iteration error: %s", err)}, nil
	}

	if len(media_ids) == 0 {
		return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("No results were fetched")}, nil
	}
	return &pb.RepeatedIdResponse{Ids: media_ids}, nil
}

func (s *DataLoaderServer) GetMediaTags(ctx context.Context, request *pb.IdRequest) (*pb.RepeatedIdResponse, error) {
	queryString := "SELECT tag_id FROM public.taggings WHERE object_id = $1"
	rows, err := s.db.Query(queryString, request.Id)
	if err != nil {
		return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("Failed to execute query: %s", err)}, nil
	}
	defer rows.Close()

	var tag_ids []int64
	for rows.Next() {
		var tag_id int64
		if err := rows.Scan(&tag_id); err != nil {
			return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("Failed to scan: %s", err)}, nil
		}
		tag_ids = append(tag_ids, tag_id)
	}

	if err := rows.Err(); err != nil {
		return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("Row iteration error: %s", err)}, nil
	}

	if len(tag_ids) == 0 {
		return &pb.RepeatedIdResponse{ErrorMessage: fmt.Sprintf("No results were fetched")}, nil
	}
	return &pb.RepeatedIdResponse{Ids: tag_ids}, nil
}
func (s *DataLoaderServer) CreateTagging(ctx context.Context, request *pb.CreateTaggingRequest) (*pb.TaggingResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.taggings WHERE object_id = $1 AND tag_id = $2", request.MediaId, request.TagId)
	var existingTagging pb.Tagging
	err := row.Scan(&existingTagging.MediaId, &existingTagging.TagId)
	if err != nil && err != sql.ErrNoRows {
		return &pb.TaggingResponse{ErrorMessage: fmt.Sprintf("Failed to fetch tagging from database: %s", err)}, nil
	}
	// Tagging already exists
	if err == nil {
		return &pb.TaggingResponse{
			Tagging: &existingTagging,
		}, nil
	}

	// Insert the new media into the database
	queryString := "INSERT INTO public.taggings (object_id, tag_id) VALUES ($1, $2) RETURNING *;"
	row = s.db.QueryRow(queryString, request.MediaId, request.TagId)

	var insertedTagging pb.Tagging
	err = row.Scan(&insertedTagging.MediaId, &insertedTagging.TagId)
	if err != nil {
		return &pb.TaggingResponse{ErrorMessage: fmt.Sprintf("Failed to insert tagging into database: %s", err)}, nil
	}

	return &pb.TaggingResponse{
		Tagging: &insertedTagging,
	}, nil
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
					ErrorMessage: fmt.Sprintf("Error: %s", err),
				}); err != nil {
					return fmt.Errorf("failed to send response: %w", err)
				}
				continue
			}
			rowsAffected, _ := res.RowsAffected()
			if err = stream.Send(&pb.CreateTaggingStreamResponse{
				Count: int64(rowsAffected),
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
				ErrorMessage: fmt.Sprintf("Error: %s", err),
			}); err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
			return nil
		}
		rowsAffected, _ := res.RowsAffected()
		if err = stream.Send(&pb.CreateTaggingStreamResponse{
			Count: int64(rowsAffected),
		}); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	// log.Println("Request correctly terminated")
	return nil
}

// !================================= Hierarchies
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
		response := &pb.HierarchyResponse{
			Hierarchy: &hierarchy,
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
		response := &pb.HierarchyResponse{ErrorMessage: "No results were fetched"}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}
func (s *DataLoaderServer) GetHierarchy(ctx context.Context, request *pb.IdRequest) (*pb.HierarchyResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.hierarchies WHERE id = $1", request.Id)
	var hierarchy pb.Hierarchy
	var nullableRootNodeID sql.NullInt64
	err := row.Scan(&hierarchy.Id, &hierarchy.Name, &hierarchy.TagSetId, &nullableRootNodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.HierarchyResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.HierarchyResponse{ErrorMessage: fmt.Sprintf("Failed to fetch hierarchy from database: %s", err)}, nil
	}
	if nullableRootNodeID.Valid {
		hierarchy.RootNodeId = nullableRootNodeID.Int64
	}
	return &pb.HierarchyResponse{
		Hierarchy: &hierarchy,
	}, nil

}
func (s *DataLoaderServer) CreateHierarchy(ctx context.Context, request *pb.CreateHierarchyRequest) (*pb.HierarchyResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.hierarchies WHERE name = $1 AND tagset_id = $2", request.Name, request.TagSetId)
	var hierarchy pb.Hierarchy
	var nullableRootNodeID sql.NullInt64
	err := row.Scan(&hierarchy.Id, &hierarchy.Name, &hierarchy.TagSetId, &nullableRootNodeID)
	if err != nil && err != sql.ErrNoRows {
		return &pb.HierarchyResponse{ErrorMessage: fmt.Sprintf("Failed to fetch hierarchy from database: %s", err)}, nil
	}
	if err == nil {
		if nullableRootNodeID.Valid {
			hierarchy.RootNodeId = nullableRootNodeID.Int64
		}
		return &pb.HierarchyResponse{
			Hierarchy: &hierarchy,
		}, nil
	}

	// Insert the new hierarchy into the database
	queryString := "INSERT INTO public.hierarchies (name, tagset_id) VALUES ($1, $2) RETURNING id, name, tagset_id;"
	row = s.db.QueryRow(queryString, request.Name, request.TagSetId)

	var insertedHierarchy pb.Hierarchy
	err = row.Scan(&insertedHierarchy.Id, &insertedHierarchy.Name, &insertedHierarchy.TagSetId)
	if err != nil {
		return &pb.HierarchyResponse{ErrorMessage: fmt.Sprintf("Failed to insert hierarchy into database: %s", err)}, nil
	}

	return &pb.HierarchyResponse{
		Hierarchy: &insertedHierarchy,
	}, nil
}

// !================================= Nodes
func (s *DataLoaderServer) GetNode(ctx context.Context, request *pb.IdRequest) (*pb.NodeResponse, error) {
	row := s.db.QueryRow("SELECT * FROM public.nodes WHERE id = $1", request.Id)

	var node pb.Node
	var nullableParentNodeID sql.NullInt64
	err := row.Scan(&node.Id, &node.TagId, &node.HierarchyId, &nullableParentNodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return &pb.NodeResponse{ErrorMessage: "No results were fetched"}, nil
		}
		return &pb.NodeResponse{ErrorMessage: fmt.Sprintf("Failed to fetch hierarchy from database: %s", err)}, nil
	}
	if nullableParentNodeID.Valid {
		node.ParentNodeId = nullableParentNodeID.Int64
	}
	return &pb.NodeResponse{
		Node: &node,
	}, nil
}
func (s *DataLoaderServer) GetNodes(request *pb.GetNodesRequest, stream pb.DataLoader_GetNodesServer) error {
	queryString := "SELECT * FROM public.nodes"
	rowcount := 0

	if request.HierarchyId > 0 || request.TagId > 0 || request.ParentNodeId > 0 {
		queryString += " WHERE"
		if request.HierarchyId > 0 {
			queryString += fmt.Sprintf(" hierarchy_id = %d AND", request.HierarchyId)
		}
		if request.TagId > 0 {
			queryString += fmt.Sprintf(" tag_id = %d AND", request.TagId)
		}
		if request.ParentNodeId > 0 {
			queryString += fmt.Sprintf(" parentnode_id = %d AND", request.ParentNodeId)
		}
		queryString = queryString[:len(queryString)-3]
	}

	rows, err := s.db.Query(queryString)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var node pb.Node
		var nullableParentNodeID sql.NullInt64
		if err := rows.Scan(&node.Id, &node.TagId, &node.HierarchyId, &nullableParentNodeID); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if nullableParentNodeID.Valid {
			node.ParentNodeId = nullableParentNodeID.Int64
		}
		response := &pb.NodeResponse{Node: &node}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
		rowcount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	if rowcount == 0 {
		response := &pb.NodeResponse{ErrorMessage: "No results were fetched"}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}
func (s *DataLoaderServer) CreateNode(ctx context.Context, request *pb.CreateNodeRequest) (*pb.NodeResponse, error) {
	// If we are trying to add a rootnode, the operations are a bit different
	// First, check if node already exists
	queryString := "SELECT * FROM public.nodes WHERE tag_id = $1 AND hierarchy_id = $2"
	row := s.db.QueryRow(queryString, request.TagId, request.HierarchyId)
	var newNode pb.Node
	var existingParentnode sql.NullInt64
	err := row.Scan(&newNode.Id, &newNode.TagId, &newNode.HierarchyId, &existingParentnode)
	if err != nil && err != sql.ErrNoRows {
		return &pb.NodeResponse{ErrorMessage: fmt.Sprintf("Failed to fetch node from database: %s", err)}, nil

	}
	if request.ParentNodeId == 0 {
		// We are trying to add a rootnode
		if err == sql.ErrNoRows {
			// If node nonè-existent we add it, retrieve its ID and update the hierarchy's rootnode_id value
			queryString := "INSERT INTO public.nodes (tag_id, hierarchy_id) VALUES ($1, $2) RETURNING *;"
			row := s.db.QueryRow(queryString, request.TagId, request.HierarchyId)
			var tmpNullParentNode sql.NullInt64
			err := row.Scan(&newNode.Id, &newNode.TagId, &newNode.HierarchyId, &tmpNullParentNode)
			if err != nil && err != sql.ErrNoRows {
				return &pb.NodeResponse{ErrorMessage: fmt.Sprintf("Failed to fetch node from database: %s", err)}, nil
			}
			queryString = "UPDATE public.hierarchies SET rootnode_id = $1 WHERE id = $2"
			result, err := s.db.Exec(queryString, newNode.Id, request.HierarchyId)
			if err != nil {
				return &pb.NodeResponse{ErrorMessage: fmt.Sprintf("Failed to execute query: %s", err)}, nil
			}
			if _, err := result.RowsAffected(); err != nil {
				return &pb.NodeResponse{ErrorMessage: fmt.Sprintf("Failed to update rootnode of hierarchy: %s", err)}, nil
			}
		} else if existingParentnode.Valid {
			return &pb.NodeResponse{ErrorMessage: "Node already present in hierarchy with a different parent node."}, nil
		}
	} else {
		// We are trying to add a non-root node
		if err == sql.ErrNoRows {
			queryString := "INSERT INTO public.nodes (tag_id, hierarchy_id, parentnode_id) VALUES ($1, $2, $3) RETURNING *;"
			row := s.db.QueryRow(queryString, request.TagId, request.HierarchyId, request.ParentNodeId)
			err := row.Scan(&newNode.Id, &newNode.TagId, &newNode.HierarchyId, &newNode.ParentNodeId)
			if err != nil && err != sql.ErrNoRows {
				return &pb.NodeResponse{ErrorMessage: fmt.Sprintf("Failed to fetch node from database: %s", err)}, nil
			}
		} else if !existingParentnode.Valid || existingParentnode.Int64 != request.ParentNodeId {
			return &pb.NodeResponse{ErrorMessage: "Node already present in hierarchy with a different parent node."}, nil
		} else {
			newNode.ParentNodeId = existingParentnode.Int64
		}

	}
	return &pb.NodeResponse{
		Node: &newNode,
	}, nil
}

func (s *DataLoaderServer) DeleteNode(ctx context.Context, request *pb.IdRequest) (*pb.StatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteNode not implemented")
}

// !================================= Other
func (s *DataLoaderServer) ResetDatabase(ctx context.Context, request *pb.EmptyRequest) (*pb.StatusResponse, error) {
	ddlSQL, err := os.ReadFile("../../ddl.sql")
	if err != nil {
		return &pb.StatusResponse{ErrorMessage: fmt.Sprintf("Failed to read DDL file: %s", err.Error())}, nil
	}

	_, err = s.db.Exec(string(ddlSQL))
	if err != nil {
		return &pb.StatusResponse{ErrorMessage: fmt.Sprintf("Failed to execute DDL: %s", err.Error())}, nil
	}

	fmt.Println("DB has been reset")
	response := &pb.StatusResponse{}
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
