package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"

	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	status "google.golang.org/grpc/status"

	_ "github.com/lib/pq"

	pb "m3.dataloader/dataloader"

	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
)

var (
	dbname     = mustGetEnv("DB_NAME")
	user       = mustGetEnv("DB_USER")
	pwd        = mustGetEnv("DB_PASSWORD")
	db_host    = mustGetEnv("DB_HOST")
	db_port    = mustGetEnvInt("DB_PORT")
	sv_host    = mustGetEnv("SV_HOST")
	sv_port    = mustGetEnvInt("SV_PORT")
	BATCH_SIZE = mustGetEnvInt("BATCH_SIZE")
)

func toInt(s string) (int, error) {
	if i, err := strconv.Atoi(s); err == nil {
		return i, nil
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	return int(f), nil
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	return value
}

func mustGetEnvInt(key string) int {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

var (
	prod *rmq.Producer
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

	// Retry pinging the database until it is ready
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		if err := db.Ping(); err != nil {
			log.Printf("Database not ready yet (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Println("Database connection established")
		return &DataLoaderServer{
			db: db,
		}, nil
	}

	db.Close()
	return nil, fmt.Errorf("failed to ping the database after %d attempts", maxRetries)
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

	body := fmt.Sprintf(`{
		"ID": "%d",
		"MediaURI": "%s",
		"ThumbnailURI": "%s"
	}`, insertedMedia.Id, insertedMedia.FileUri, insertedMedia.ThumbnailUri)
	rmq.PublishMessage(prod, body, fmt.Sprintf("media.%d", insertedMedia.FileType))

	return &insertedMedia, nil
}

func (s *DataLoaderServer) CreateMediaStream(stream pb.DataLoader_CreateMediaStreamServer) error {
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
			queryString = queryString[:len(queryString)-1] + "RETURNING *;"
			res, err := s.db.Query(queryString, data...)
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
			for res.Next() {
				var insertedMedia pb.Media
				if err := res.Scan(&insertedMedia.Id, &insertedMedia.FileUri, &insertedMedia.FileType, &insertedMedia.ThumbnailUri); err != nil {
					log.Printf("Error scanning row: %s", err)
					continue
				}
				body := fmt.Sprintf(`{
					"ID": "%d",
					"MediaURI": "%s",
					"ThumbnailURI": "%s"
				}`, insertedMedia.Id, insertedMedia.FileUri, insertedMedia.ThumbnailUri)
				rmq.PublishMessage(prod, body, fmt.Sprintf("media.%d", insertedMedia.FileType))
			}
		}
	}

	if requestCounter%BATCH_SIZE > 0 {
		queryString = queryString[:len(queryString)-1] + "RETURNING *;"
		res, err := s.db.Query(queryString, data...)
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
		for res.Next() {
			var insertedMedia pb.Media
			if err := res.Scan(&insertedMedia.Id, &insertedMedia.FileUri, &insertedMedia.FileType, &insertedMedia.ThumbnailUri); err != nil {
				log.Printf("Error scanning row: %s", err)
				continue
			}
			body := fmt.Sprintf(`{
				"ID": "%d",
				"MediaURI": "%s",
				"ThumbnailURI": "%s"
			}`, insertedMedia.Id, insertedMedia.FileUri, insertedMedia.ThumbnailUri)
			rmq.PublishMessage(prod, body, fmt.Sprintf("media.%d", insertedMedia.FileType))
		}
	}
	return nil
}

// CreateMediaWithTags creates a media, attaches all provided tags in a single call,
// then publishes one enriched RabbitMQ message containing both the media info and all tags.
// RabbitMQ routing key: media.<file_type>
// Message format:
//
//	{
//	  "ID": "25",
//	  "MediaURI": "http://...",
//	  "ThumbnailURI": "http://...",
//	  "Tags": {
//	    "Timestamp UTC": "2015-02-23 00:37",
//	    "Timezone": "Europe/Dublin",
//	    ...
//	  }
//	}
func (s *DataLoaderServer) CreateMediaWithTags(ctx context.Context, request *pb.MediaWithTagsRequest) (*pb.Media, error) {
	// ── 1. Insert or retrieve the media ─────────────────────────────────
	row := s.db.QueryRow("SELECT * FROM public.medias WHERE file_uri = $1", request.FileUri)
	var insertedMedia pb.Media
	err := row.Scan(&insertedMedia.Id, &insertedMedia.FileUri, &insertedMedia.FileType, &insertedMedia.ThumbnailUri)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch media from database: %s", err)
	}

	if err == sql.ErrNoRows {
		// Media does not exist yet — insert it
		row = s.db.QueryRow(
			"INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES ($1, $2, $3) RETURNING *;",
			request.FileUri, request.FileType, request.ThumbnailUri,
		)
		err = row.Scan(&insertedMedia.Id, &insertedMedia.FileUri, &insertedMedia.FileType, &insertedMedia.ThumbnailUri)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to insert media into database: %s", err)
		}
	} else {
		// Media already exists — check for conflicts
		if insertedMedia.FileType != request.FileType || insertedMedia.ThumbnailUri != request.ThumbnailUri {
			return nil, status.Errorf(codes.AlreadyExists,
				"Media URI '%s' already exists with a different type or thumbnail_uri", request.FileUri)
		}
	}

	// ── 2. Collect tag values for the RabbitMQ message (no DB insertion) ─
	tagMap := make(map[string]string) // tagset_name -> value (for the RabbitMQ message)

	for _, entry := range request.Tags {
		if entry.Value == "" {
			continue
		}
		tagMap[entry.TagsetName] = entry.Value
	}

	// ── 3. Build and publish the enriched RabbitMQ message ──────────────
	tagsJSON, err := json.Marshal(tagMap)
	if err != nil {
		log.Printf("CreateMediaWithTags: failed to marshal tags map: %s", err)
		tagsJSON = []byte("{}")
	}

	body := fmt.Sprintf(`{
		"ID": "%d",
		"MediaURI": "%s",
		"ThumbnailURI": "%s",
		"Tags": %s
	}`, insertedMedia.Id, insertedMedia.FileUri, insertedMedia.ThumbnailUri, string(tagsJSON))

	rmq.PublishMessage(prod, body, fmt.Sprintf("media.%d", insertedMedia.FileType))

	return &insertedMedia, nil
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

	// Insert the new tagset into the database
	queryString := "INSERT INTO public.tagsets (name, tagtype_id) VALUES ($1, $2) RETURNING *;"
	row = s.db.QueryRow(queryString, request.Name, request.TagTypeId)

	var insertedTagset pb.TagSet
	err = row.Scan(&insertedTagset.Id, &insertedTagset.Name, &insertedTagset.TagTypeId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert tagset into database: %s", err)
	}

	return &insertedTagset, nil
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
		response := &pb.StreamingTagResponse{}
		switch tagtype_id {
		case 1:
			if text_value.Valid {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Tag{
						Tag: &pb.Tag{
							Id:        id,
							TagSetId:  tagset_id,
							TagTypeId: tagtype_id,
							Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: text_value.String}},
						},
					},
				}
			} else {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Error{
						Error: status.Newf(codes.Internal, "Null value for tag id=%d", id).Proto(),
					},
				}
			}
		case 2:
			if timestamp_value.Valid {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Tag{
						Tag: &pb.Tag{
							Id:        id,
							TagSetId:  tagset_id,
							TagTypeId: tagtype_id,
							Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: timestamp_value.String}},
						},
					},
				}
			} else {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Error{
						Error: status.Newf(codes.Internal, "Null value for tag id=%d", id).Proto(),
					},
				}
			}
		case 3:
			if time_value.Valid {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Tag{
						Tag: &pb.Tag{
							Id:        id,
							TagSetId:  tagset_id,
							TagTypeId: tagtype_id,
							Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: time_value.String}},
						},
					},
				}
			} else {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Error{
						Error: status.Newf(codes.Internal, "Null value for tag id=%d", id).Proto(),
					},
				}
			}
		case 4:
			if date_value.Valid {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Tag{
						Tag: &pb.Tag{
							Id:        id,
							TagSetId:  tagset_id,
							TagTypeId: tagtype_id,
							Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: date_value.String}},
						},
					},
				}
			} else {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Error{
						Error: status.Newf(codes.Internal, "Null value for tag id=%d", id).Proto(),
					},
				}
			}
		case 5:
			if num_value.Valid {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Tag{
						Tag: &pb.Tag{
							Id:        id,
							TagSetId:  tagset_id,
							TagTypeId: tagtype_id,
							Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: num_value.Int64}},
						},
					},
				}
			} else {
				response = &pb.StreamingTagResponse{
					Message: &pb.StreamingTagResponse_Error{
						Error: status.Newf(codes.Internal, "Null value for tag id=%d", id).Proto(),
					},
				}
			}
		default:
			response = &pb.StreamingTagResponse{
				Message: &pb.StreamingTagResponse_Error{
					Error: status.Newf(codes.Internal, "Incorrect type for tag id=%d", id).Proto(),
				},
			}
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
		if request.TagTypeId > 0 && request.TagSetId > 0 {
			s = status.Newf(codes.NotFound, "No tags found with tag type ID %d and tag set ID %d", request.TagTypeId, request.TagSetId)
		} else if request.TagTypeId > 0 {
			s = status.Newf(codes.NotFound, "No tags found with tag type ID %d", request.TagTypeId)
		} else if request.TagSetId > 0 {
			s = status.Newf(codes.NotFound, "No tags found with tag set ID %d", request.TagSetId)
		} else {
			s = status.Newf(codes.NotFound, "No tags found")
		}

		response := &pb.StreamingTagResponse{
			Message: &pb.StreamingTagResponse_Error{
				Error: s.Proto(),
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}

func (s *DataLoaderServer) GetTag(ctx context.Context, request *pb.IdRequest) (*pb.Tag, error) {
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
			return nil, status.Errorf(codes.NotFound, "No tag found with ID %d", request.Id)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch tag from database: %s", err)
	}

	switch tagtype_id {
	case 1:
		return &pb.Tag{
			Id:        int64(tag_id),
			TagSetId:  int64(tagset_id),
			TagTypeId: int64(tagtype_id),
			Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: value}},
		}, nil
	case 2:
		return &pb.Tag{
			Id:        int64(tag_id),
			TagSetId:  int64(tagset_id),
			TagTypeId: int64(tagtype_id),
			Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: value}},
		}, nil
	case 3:
		return &pb.Tag{
			Id:        int64(tag_id),
			TagSetId:  int64(tagset_id),
			TagTypeId: int64(tagtype_id),
			Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: value}},
		}, nil
	case 4:
		return &pb.Tag{
			Id:        int64(tag_id),
			TagSetId:  int64(tagset_id),
			TagTypeId: int64(tagtype_id),
			Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: value}},
		}, nil
	case 5:
		num_value, err := strconv.Atoi(value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Error converting tag value to integer: %s", err)
		}
		return &pb.Tag{
			Id:        int64(tag_id),
			TagSetId:  int64(tagset_id),
			TagTypeId: int64(tagtype_id),
			Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: int64(num_value)}},
		}, nil
	}
	return nil, status.Errorf(codes.DataLoss, "Invalid tag type was fetched: %s\nCheck database integrity.", err)
}

func (s *DataLoaderServer) CreateTag(ctx context.Context, request *pb.CreateTagRequest) (*pb.Tag, error) {
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
		return nil, status.Errorf(codes.InvalidArgument, "Invalid tag type provided: range is 1-5")
	}

	row := s.db.QueryRow(queryString, data...)
	var tag_id, tagtype_id, tagset_id int
	var value string
	err := row.Scan(&tag_id, &tagtype_id, &tagset_id, &value)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch tag from database: %s", err)
	}
	if err == nil {
		switch tagtype_id {
		case 1:
			return &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: value}},
			}, nil
		case 2:
			return &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: value}},
			}, nil
		case 3:
			return &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: value}},
			}, nil
		case 4:
			return &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: value}},
			}, nil
		case 5:
			num_value, err := strconv.Atoi(value)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "Error converting tag value to integer: %s", err)
			}
			return &pb.Tag{
				Id:        int64(tag_id),
				TagSetId:  int64(tagset_id),
				TagTypeId: int64(tagtype_id),
				Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: int64(num_value)}},
			}, nil
		}
	}

	// If non existent and no type issues, create the new tag
	var insertedId int64
	queryString = "INSERT INTO public.tags (tagtype_id, tagset_id) VALUES ($1, $2) RETURNING id"
	row = s.db.QueryRow(queryString, request.TagTypeId, request.TagSetId)
	err = row.Scan(&insertedId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert tag into database: %s", err)
	}

	queryString = "INSERT INTO public."
	switch request.TagTypeId {
	case 1:
		var value string
		queryString += "alphanumerical_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name"
		row = s.db.QueryRow(queryString, insertedId, request.GetAlphanumerical().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to insert tag into database: %s", err)
		}
		return &pb.Tag{
			Id:        insertedId,
			TagSetId:  request.TagSetId,
			TagTypeId: request.TagTypeId,
			Value:     &pb.Tag_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: value}},
		}, nil
	case 2:
		var value string
		queryString += "timestamp_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name::text"
		row = s.db.QueryRow(queryString, insertedId, request.GetTimestamp().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to insert tag into database: %s", err)
		}
		return &pb.Tag{
			Id:        insertedId,
			TagSetId:  request.TagSetId,
			TagTypeId: request.TagTypeId,
			Value:     &pb.Tag_Timestamp{Timestamp: &pb.TimeStampValue{Value: value}},
		}, nil
	case 3:
		var value string
		queryString += "time_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name::text"
		row = s.db.QueryRow(queryString, insertedId, request.GetTime().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to insert tag into database: %s", err)
		}
		return &pb.Tag{
			Id:        insertedId,
			TagSetId:  request.TagSetId,
			TagTypeId: request.TagTypeId,
			Value:     &pb.Tag_Time{Time: &pb.TimeValue{Value: value}},
		}, nil
	case 4:
		var value string
		queryString += "date_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name::text"
		row = s.db.QueryRow(queryString, insertedId, request.GetDate().Value, request.TagSetId)
		err = row.Scan(&value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to insert tag into database: %s", err)
		}
		return &pb.Tag{
			Id:        insertedId,
			TagSetId:  request.TagSetId,
			TagTypeId: request.TagTypeId,
			Value:     &pb.Tag_Date{Date: &pb.DateValue{Value: value}},
		}, nil
	case 5:
		var value int64
		queryString += "numerical_tags (id, name, tagset_id) VALUES ($1, $2, $3) RETURNING name"
		row = s.db.QueryRow(queryString, insertedId, request.GetNumerical().Value, request.TagSetId)
		err = row.Scan(&value)
		log.Printf("Inserted numerical tag with id, name, tagset_id: %d %d %d", insertedId, request.GetNumerical().Value, request.TagSetId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to insert tag into database: %s", err)
		}
		return &pb.Tag{
			Id:        insertedId,
			TagSetId:  request.TagSetId,
			TagTypeId: request.TagTypeId,
			Value:     &pb.Tag_Numerical{Numerical: &pb.NumericalValue{Value: value}},
		}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid tag type provided: range is 1-5")
	}
}

func (s *DataLoaderServer) ChangeTagName(ctx context.Context, request *pb.ChangeTagNameRequest) (*pb.Empty, error) {
	// Validate the request
	if request == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Request cannot be nil")
	}

	// Get tagset id and tagtype id from tagset name
	var tagsetId, tagtypeId int64
	err := s.db.QueryRow("SELECT id, tagtype_id FROM public.tagsets WHERE name = $1", request.TagsetName).Scan(&tagsetId, &tagtypeId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to retrieve tagset from database: %s", err)
	}

	// Get tag id from tag name, depending on tagtype
	var tagId int64
	switch tagtypeId {
	case 1:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.alphanumerical_tags ant ON t.id = ant.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND ant.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
	case 2:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.timestamp_tags tst ON t.id = tst.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND tst.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
	case 3:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.time_tags tt ON t.id = tt.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND tt.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
	case 4:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.date_tags dt ON t.id = dt.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND dt.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
	case 5:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.numerical_tags nt ON t.id = nt.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND nt.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to retrieve tag from database: %s", err)
	}

	queryString := "UPDATE public."
	var body string
	switch tagtypeId {
	case 1:
		queryString += "alphanumerical_tags SET name = $1 WHERE id = $2"
		newVal := request.NewName
		if v := request.GetNewAlphanumerical(); v != nil {
			newVal = v.Value
		}
		_, err = s.db.Exec(queryString, newVal, tagId)
		body = fmt.Sprintf(`{
			"OldName": "%s",
			"NewName": "%s"
			}`, request.TagName, newVal)
	case 2:
		queryString += "timestamp_tags SET name = $1 WHERE id = $2"
		newVal := request.NewName
		if v := request.GetNewTimestamp(); v != nil {
			newVal = v.Value
		}
		_, err = s.db.Exec(queryString, newVal, tagId)
		body = fmt.Sprintf(`{
			"OldName": "%s",
			"NewName": "%s"
			}`, request.TagName, newVal)
	case 3:
		queryString += "time_tags SET name = $1 WHERE id = $2"
		newVal := request.NewName
		if v := request.GetNewTime(); v != nil {
			newVal = v.Value
		}
		_, err = s.db.Exec(queryString, newVal, tagId)
		body = fmt.Sprintf(`{
			"OldName": "%s",
			"NewName": "%s"
			}`, request.TagName, newVal)
	case 4:
		queryString += "date_tags SET name = $1 WHERE id = $2"
		newVal := request.NewName
		if v := request.GetNewDate(); v != nil {
			newVal = v.Value
		}
		_, err = s.db.Exec(queryString, newVal, tagId)
		body = fmt.Sprintf(`{
			"OldName": "%s",
			"NewName": "%s"
			}`, request.TagName, newVal)
	case 5:
		queryString += "numerical_tags SET name = $1 WHERE id = $2"
		if v := request.GetNewNumerical(); v != nil {
			_, err = s.db.Exec(queryString, v.Value, tagId)
			body = fmt.Sprintf(`{
				"OldName": "%s",
				"NewName": "%d"
				}`, request.TagName, v.Value)
		} else {
			_, err = s.db.Exec(queryString, request.NewName, tagId)
			body = fmt.Sprintf(`{
				"OldName": "%s",
				"NewName": "%s"
				}`, request.TagName, request.NewName)
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid tag type provided: range is 1-5")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update tag in database: %s", err)
	}

	rmq.PublishMessage(prod, body, fmt.Sprintf("tag_update.%s", request.TagsetName))

	return &pb.Empty{}, nil
}

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

	// Insert the new tagging into the database
	queryString := "INSERT INTO public.taggings (object_id, tag_id) VALUES ($1, $2) RETURNING *;"
	row = s.db.QueryRow(queryString, request.MediaId, request.TagId)

	var insertedTagging pb.Tagging
	err = row.Scan(&insertedTagging.MediaId, &insertedTagging.TagId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert tagging into database: %s", err)
	}
	queryString = `SELECT 
		COALESCE(ant.name, tst.name::text, tt.name::text, dt.name::text, nt.name::text) AS value,
		t.tagtype_id,
		ts.name AS tagset_name
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
		public.numerical_tags nt ON t.id = nt.id
	JOIN 
		public.tagsets ts ON t.tagset_id = ts.id
	WHERE 
		t.id = $1;`

	var tagValue string
	var tagTypeId int64
	var tagSetName string
	err = s.db.QueryRow(queryString, request.TagId).Scan(&tagValue, &tagTypeId, &tagSetName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to fetch tag value and type from database: %s", err)
	}

	body := fmt.Sprintf(`{
		"taggingValue": "%s",
		"mediaID": "%d"
	}`, tagValue, insertedTagging.MediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.already_added.%d.%s", tagTypeId, tagSetName))

	return &insertedTagging, nil
}

func (s *DataLoaderServer) CreateTaggingStream(stream pb.DataLoader_CreateTaggingStreamServer) error {
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
	return nil
}

func (s *DataLoaderServer) ChangeTagging(ctx context.Context, request *pb.ChangeTaggingRequest) (*pb.Empty, error) {

	var mediaId int64
	err := s.db.QueryRow("SELECT id FROM public.medias WHERE file_uri = $1", request.MediaUri).Scan(&mediaId)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no media found with URI %s", request.MediaUri)
		}
		return nil, fmt.Errorf("failed to fetch media ID from database: %w", err)
	}

	// Get tagset id and tagtype id from tagset name
	var tagsetId, tagtypeId int64
	err = s.db.QueryRow("SELECT id, tagtype_id FROM public.tagsets WHERE name = $1", request.TagsetName).Scan(&tagsetId, &tagtypeId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to retrieve tagset from database: %s", err)
	}
	var tagRequest *pb.CreateTagRequest

	var tagId int64
	switch tagtypeId {
	case 1:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.alphanumerical_tags ant ON t.id = ant.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND ant.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
		newVal := request.NewName
		if v := request.GetAlphanumerical(); v != nil {
			newVal = v.GetValue()
		}
		tagRequest = &pb.CreateTagRequest{
			TagTypeId: int64(tagtypeId),
			TagSetId:  tagsetId,
			Value:     &pb.CreateTagRequest_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: newVal}},
		}
	case 2:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.timestamp_tags tst ON t.id = tst.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND tst.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
		newVal := request.NewName
		if v := request.GetTimestamp(); v != nil {
			newVal = v.GetValue()
		}
		tagRequest = &pb.CreateTagRequest{
			TagTypeId: int64(tagtypeId),
			TagSetId:  tagsetId,
			Value:     &pb.CreateTagRequest_Timestamp{Timestamp: &pb.TimeStampValue{Value: newVal}},
		}
	case 3:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.time_tags tt ON t.id = tt.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND tt.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
		newVal := request.NewName
		if v := request.GetTime(); v != nil {
			newVal = v.GetValue()
		}
		tagRequest = &pb.CreateTagRequest{
			TagTypeId: int64(tagtypeId),
			TagSetId:  tagsetId,
			Value:     &pb.CreateTagRequest_Time{Time: &pb.TimeValue{Value: newVal}},
		}
	case 4:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.date_tags dt ON t.id = dt.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND dt.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
		newVal := request.NewName
		if v := request.GetDate(); v != nil {
			newVal = v.GetValue()
		}
		tagRequest = &pb.CreateTagRequest{
			TagTypeId: int64(tagtypeId),
			TagSetId:  tagsetId,
			Value:     &pb.CreateTagRequest_Date{Date: &pb.DateValue{Value: newVal}},
		}
	case 5:
		err = s.db.QueryRow(
			"SELECT t.id FROM public.tags t JOIN public.numerical_tags nt ON t.id = nt.id WHERE t.tagset_id = $1 AND t.tagtype_id = $2 AND nt.name = $3",
			tagsetId, tagtypeId, request.TagName).Scan(&tagId)
		if v := request.GetNumerical(); v != nil {
			tagRequest = &pb.CreateTagRequest{
				TagTypeId: int64(tagtypeId),
				TagSetId:  tagsetId,
				Value:     &pb.CreateTagRequest_Numerical{Numerical: &pb.NumericalValue{Value: int64(v.GetValue())}},
			}
		} else {
			tagRequest = &pb.CreateTagRequest{
				TagTypeId: int64(tagtypeId),
				TagSetId:  tagsetId,
				Value:     &pb.CreateTagRequest_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: request.NewName}},
			}
		}
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to retrieve tag from database: %s", err)
	}
	query := "SELECT 1 FROM public.taggings WHERE object_id = $1 AND tag_id = $2"
	var tmp int
	err = s.db.QueryRow(query, mediaId, tagId).Scan(&tmp)
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.InvalidArgument, "Tagging with media ID %d and tag ID %d does not exist", mediaId, tagId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to fetch tagging from database: %s", err)
	}

	body := fmt.Sprintf(`{
			"OldName": "%s",
			"NewName": "%s",
			"MediaId": "%d"
			}`, request.TagName, request.NewName, mediaId)

	tag, err := s.CreateTag(context.Background(), tagRequest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create tag: %s", err)
	}
	// Update the tagging in the database
	queryString := "UPDATE public.taggings SET tag_id = $1 WHERE tag_id = $2 AND object_id = $3"
	_, err = s.db.Exec(queryString, tag.Id, tagId, mediaId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update tagging in database: %s", err)
	}

	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging_update.%s", request.TagsetName))

	return &pb.Empty{}, nil
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

// !================================= Nodes
func (s *DataLoaderServer) GetNode(ctx context.Context, request *pb.IdRequest) (*pb.Node, error) {
	row := s.db.QueryRow("SELECT * FROM public.nodes WHERE id = $1", request.Id)

	var node pb.Node
	var nullableParentNodeID sql.NullInt64
	err := row.Scan(&node.Id, &node.TagId, &node.HierarchyId, &nullableParentNodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "No node found with ID %d", request.Id)
		}
		return nil, status.Errorf(codes.Internal, "Failed to fetch node from database: %s", err)
	}
	if nullableParentNodeID.Valid {
		node.ParentNodeId = nullableParentNodeID.Int64
	}
	return &node, nil
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
		response := &pb.StreamingNodeResponse{
			Message: &pb.StreamingNodeResponse_Node{
				Node: &node,
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
		if request.HierarchyId > 0 && request.TagId > 0 && request.ParentNodeId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with hierarchy ID %d, tag ID %d, and parent node ID %d", request.HierarchyId, request.TagId, request.ParentNodeId)
		} else if request.HierarchyId > 0 && request.TagId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with hierarchy ID %d and tag ID %d", request.HierarchyId, request.TagId)
		} else if request.HierarchyId > 0 && request.ParentNodeId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with hierarchy ID %d and parent node ID %d", request.HierarchyId, request.ParentNodeId)
		} else if request.TagId > 0 && request.ParentNodeId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with tag ID %d and parent node ID %d", request.TagId, request.ParentNodeId)
		} else if request.HierarchyId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with hierarchy ID %d", request.HierarchyId)
		} else if request.TagId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with tag ID %d", request.TagId)
		} else if request.ParentNodeId > 0 {
			s = status.Newf(codes.NotFound, "No nodes found with parent node ID %d", request.ParentNodeId)
		} else {
			s = status.Newf(codes.NotFound, "No nodes found")
		}
		response := &pb.StreamingNodeResponse{
			Message: &pb.StreamingNodeResponse_Error{
				Error: s.Proto(),
			},
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}
	return nil
}

func (s *DataLoaderServer) CreateNode(ctx context.Context, request *pb.CreateNodeRequest) (*pb.Node, error) {
	// First, check if node already exists
	queryString := "SELECT * FROM public.nodes WHERE tag_id = $1 AND hierarchy_id = $2"
	row := s.db.QueryRow(queryString, request.TagId, request.HierarchyId)
	var newNode pb.Node
	var existingParentnode sql.NullInt64
	err := row.Scan(&newNode.Id, &newNode.TagId, &newNode.HierarchyId, &existingParentnode)
	if err != nil && err != sql.ErrNoRows {
		return nil, status.Errorf(codes.Internal, "Failed to fetch node from database: %s", err)
	}
	if request.ParentNodeId == 0 {
		// We are trying to add a rootnode
		if err == sql.ErrNoRows {
			// If node non-existent we add it, retrieve its ID and update the hierarchy's rootnode_id value
			queryString := "INSERT INTO public.nodes (tag_id, hierarchy_id) VALUES ($1, $2) RETURNING *;"
			row := s.db.QueryRow(queryString, request.TagId, request.HierarchyId)
			var tmpNullParentNode sql.NullInt64
			err := row.Scan(&newNode.Id, &newNode.TagId, &newNode.HierarchyId, &tmpNullParentNode)
			if err != nil && err != sql.ErrNoRows {
				return nil, status.Errorf(codes.Internal, "Failed to fetch node from database: %s", err)
			}
			queryString = "UPDATE public.hierarchies SET rootnode_id = $1 WHERE id = $2"
			result, err := s.db.Exec(queryString, newNode.Id, request.HierarchyId)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to execute query: %s", err)
			}
			if _, err := result.RowsAffected(); err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to update rootnode of hierarchy: %s", err)
			}
		} else if existingParentnode.Valid {
			return nil, status.Errorf(codes.AlreadyExists, "Node already present in hierarchy with a different parent node.")
		}
	} else {
		// We are trying to add a non-root node
		if err == sql.ErrNoRows {
			queryString := "INSERT INTO public.nodes (tag_id, hierarchy_id, parentnode_id) VALUES ($1, $2, $3) RETURNING *;"
			row := s.db.QueryRow(queryString, request.TagId, request.HierarchyId, request.ParentNodeId)
			err := row.Scan(&newNode.Id, &newNode.TagId, &newNode.HierarchyId, &newNode.ParentNodeId)
			if err != nil && err != sql.ErrNoRows {
				return nil, status.Errorf(codes.Internal, "Failed to insert node into database: %s", err)
			}
		} else if !existingParentnode.Valid || existingParentnode.Int64 != request.ParentNodeId {
			return nil, status.Errorf(codes.AlreadyExists, "Node already present in hierarchy with a different parent node.")
		} else {
			newNode.ParentNodeId = existingParentnode.Int64
		}
	}
	return &newNode, nil
}

func (s *DataLoaderServer) DeleteNode(ctx context.Context, request *pb.IdRequest) (*pb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteNode not implemented")
}

// !================================= Other
func (s *DataLoaderServer) ResetDatabase(ctx context.Context, request *pb.Empty) (*pb.Empty, error) {
	ddlPath := os.Getenv("DDL_PATH")
	if ddlPath == "" {
		ddlPath = "../../ddl.sql"
	}
	ddlSQL, err := os.ReadFile(ddlPath)
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

func makeTagRequest(tagTypeId int64, tagSetId int64, tag string) *pb.CreateTagRequest {
	var request *pb.CreateTagRequest
	switch tagTypeId {
	case 1:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: tag}},
		}
	case 2:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Timestamp{Timestamp: &pb.TimeStampValue{Value: tag}},
		}
	case 3:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Time{Time: &pb.TimeValue{Value: tag}},
		}
	case 4:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Date{Date: &pb.DateValue{Value: tag}},
		}
	case 5:
		tagInt, _ := strconv.Atoi(tag)
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Numerical{Numerical: &pb.NumericalValue{Value: (int64(tagInt))}},
		}
	}

	return request
}

func listenForTaggingMessage(server *DataLoaderServer) {
	rmq.Listen("tagging.not_added.*.*", func(msg amqp.Delivery) {
		log.Printf("Received message: %s", msg.Body)

		// Get the tag type ID and tagset from the routing key
		routingKeyParts := strings.Split(msg.RoutingKey, ".")
		if len(routingKeyParts) != 4 {
			log.Printf("Invalid routing key format: %s", msg.RoutingKey)
			return
		}

		tagTypeId, err := strconv.Atoi(routingKeyParts[2])
		if err != nil {
			log.Printf("Failed to parse tagTypeId from routing key: %s", err)
			return
		}

		tagset := routingKeyParts[3]

		// Get the information from the message body
		var message map[string]string
		err = json.Unmarshal(msg.Body, &message)
		if err != nil {
			log.Printf("Failed to parse message body: %v", err)
			return
		}

		mediaId, ok := message["mediaID"]
		if !ok {
			log.Printf("Missing mediaID in message body")
			return
		}

		tag, ok := message["taggingValue"]
		if !ok {
			log.Printf("Missing taggingValue in message body")
			return
		}

		log.Printf("Parsed message: mediaID=%s, taggingValue=%s, tagset=%s", mediaId, tag, tagset)

		// Convert mediaId (string) to int64 so we can call CreateTagging.
		mediaIdInt, err := strconv.ParseInt(mediaId, 10, 64)
		if err != nil {
			log.Printf("Invalid mediaID '%s': %v", mediaId, err)
			return
		}

		// Get/Create the tagset ID from the database
		response, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
			Name:      tagset,
			TagTypeId: int64(tagTypeId),
		})
		if err != nil {
			log.Printf("Failed to create tagset: %s", err)
			return
		}
		tagsetId := response.GetId()

		// Get/Create the tag via the gRPC CreateTag method. This ensures the
		// same code path as every other producer (geodatas, hierarchy, direct
		// gRPC calls, ...) and keeps tag normalization consistent.
		tagResp, err := server.CreateTag(
			context.Background(),
			makeTagRequest(int64(tagTypeId), tagsetId, tag),
		)
		if err != nil {
			log.Printf("Failed to create tag (tagset=%s, value=%s): %s", tagset, tag, err)
			return
		}

		// Create the tagging via the gRPC CreateTagging method. This is the
		// critical fix: CreateTagging publishes on "tagging.already_added.*.*",
		// which downstream consumers (e.g. the frontend refresh path) rely on
		// to display newly tagged media. A raw INSERT into public.taggings
		// would persist the row but skip this notification, causing tags
		// coming from the metadata/not_added pipeline to never be displayed.
		_, err = server.CreateTagging(context.Background(), &pb.CreateTaggingRequest{
			MediaId: mediaIdInt,
			TagId:   tagResp.Id,
		})
		if err != nil {
			log.Printf("Failed to create tagging (media=%d, tag=%d): %s", mediaIdInt, tagResp.Id, err)
			return
		}

		log.Printf("Tagged media=%d with %s='%s' (tagset=%s)", mediaIdInt, tagset, tag, tagset)
	})
}

func listenForHierarchyMessage(server *DataLoaderServer) {
	rmq.Listen("hierarchy", func(msg amqp.Delivery) {
		log.Printf("debug : received message: %s", msg.Body)
		// Parse the message body as JSON
		var message map[string]interface{}
		err := json.Unmarshal(msg.Body, &message)
		if err != nil {
			log.Printf("debug : failed to parse message body: %v", err)
			return
		}
		log.Printf("debug : message parsed: %+v", message)

		tagsetName, ok := message["tagset"].(string)
		if !ok {
			log.Printf("debug : missing or invalid tagset in message body")
			return
		}
		log.Printf("debug : tagsetName = %s", tagsetName)

		tagTypeId, ok := message["tagTypeId"].(float64)
		if !ok {
			log.Printf("debug : missing or invalid tagTypeId in message body : %v", message["tagTypeId"])
			return
		}
		log.Printf("debug : tagTypeId = %v", tagTypeId)

		hierarchyName, ok := message["hierarchy"].(string)
		if !ok {
			log.Printf("debug : missing or invalid hierarchy in message body")
			return
		}
		log.Printf("debug : hierarchyName = %s", hierarchyName)

		tag, ok := message["tag"].(string)
		if !ok {
			log.Printf("debug : missing or invalid tag in message body")
			return
		}
		log.Printf("debug : tag = %s", tag)

		log.Printf("debug : creating tagset")
		tagsetResponse, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
			Name:      tagsetName,
			TagTypeId: int64(tagTypeId),
		})
		if err != nil {
			log.Printf("debug : failed to create tagset: %s", err)
			return
		}
		log.Printf("debug : tagset created: %+v", tagsetResponse)

		log.Printf("debug : creating hierarchy")
		hierarchyResponse, err := server.CreateHierarchy(context.Background(), &pb.CreateHierarchyRequest{
			Name:     hierarchyName,
			TagSetId: tagsetResponse.GetId(),
		})
		if err != nil {
			log.Printf("debug : failed to create hierarchy: %s", err)
			return
		}
		log.Printf("debug : hierarchy created: %+v", hierarchyResponse)

		log.Printf("debug : creating tag")
		tagResponse, err := server.CreateTag(context.Background(), makeTagRequest(int64(tagTypeId), tagsetResponse.GetId(), tag))
		if err != nil {
			log.Printf("debug : failed to create tag: %s", err)
			return
		}
		log.Printf("debug : tag created: %+v", tagResponse)

		log.Printf("debug : creating root node")
		rootNodeResponse, err := server.CreateNode(context.Background(), &pb.CreateNodeRequest{
			TagId:        tagResponse.Id,
			HierarchyId:  hierarchyResponse.Id,
			ParentNodeId: 0, // Root node has no parent
		})
		if err != nil {
			log.Printf("debug : failed to create root node: %s", err)
			return
		}
		log.Printf("debug : root node created: %+v", rootNodeResponse)

		child, ok := message["child"].(map[string]interface{})
		parentNodeId := rootNodeResponse.Id
		log.Printf("debug : entering child node creation loop")
		for ok && len(child) > 0 {
			tagValue, okTag := child["tag"].(string)
			tagTypeId, okType := child["tagTypeId"].(float64)
			tagsetName, okTagSet := child["tagset"].(string)

			log.Printf("debug : child tagValue = %v, okTag = %v, tagTypeId = %v, okType = %v, tagsetName = %v, okTagSet = %v", tagValue, okTag, tagTypeId, okType, tagsetName, okTagSet)
			if !okTag || tagValue == "" || !okType || !okTagSet || tagsetName == "" {
				log.Printf("debug : missing or invalid child tag or tagTypeId or tagsetId in message body")
				break
			}
			log.Printf("debug : creating tagset")
			tagsetResponse, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
				Name:      tagsetName,
				TagTypeId: int64(tagTypeId),
			})
			if err != nil {
				log.Printf("debug : failed to create tagset: %s", err)
				return
			}
			log.Printf("debug : tagset created: %+v", tagsetResponse)
			log.Printf("debug : creating child tag")
			tagResp, err := server.CreateTag(context.Background(), makeTagRequest(int64(tagTypeId), tagsetResponse.GetId(), tagValue))
			if err != nil {
				log.Printf("debug : failed to create child tag: %s", err)
				break
			}
			log.Printf("debug : child tag created: %+v", tagResp)

			log.Printf("debug : creating child node")
			nodeResp, err := server.CreateNode(context.Background(), &pb.CreateNodeRequest{
				TagId:        tagResp.Id,
				HierarchyId:  hierarchyResponse.Id,
				ParentNodeId: parentNodeId,
			})
			if err != nil {
				log.Printf("debug : failed to create child node: %s", err)
				break
			}
			log.Printf("debug : child node created: %+v", nodeResp)
			parentNodeId = nodeResp.Id

			nextChild, okNext := child["child"].(map[string]interface{})
			log.Printf("debug : moving to next child: okNext = %v, nextChild = %+v", okNext, nextChild)
			child = nextChild
			ok = okNext && len(child) > 0
		}
		log.Printf("debug : finished processing hierarchy message")
	})
}

// listenForGeodatasMessage listens on geodatas.new and creates tags/taggings
// for the following fields, stored in distinct tagsets:
//   - Country        (tagset "Country",       alphanumerical)
//   - City           (tagset "City",           alphanumerical)
//   - Location Type  (tagset "Location Type",  alphanumerical) — OSM type of the location
//   - POI Category   (tagset "POI Category",   alphanumerical) — category of nearby POIs
//
// geodatas.known messages are logged but ignored: the location is already known
// and its tags are already in the database.
func listenForGeodatasMessage(server *DataLoaderServer) {
	rmq.Listen("geodatas.new", func(msg amqp.Delivery) {
		log.Printf("[geodatas] received message: %s", msg.Body)

		var message map[string]interface{}
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("[geodatas] failed to parse message body: %v", err)
			return
		}

		// ── Extract media_id ──────────────────────────────────────────────
		mediaIdRaw, ok := message["media_id"]
		if !ok {
			log.Printf("[geodatas] missing media_id in message body")
			return
		}
		mediaIdStr := fmt.Sprintf("%v", mediaIdRaw)
		mediaId, err := strconv.ParseInt(mediaIdStr, 10, 64)
		if err != nil {
			log.Printf("[geodatas] invalid media_id '%s': %v", mediaIdStr, err)
			return
		}

		// helper: get or create a tagset, then get or create a tag, then create the tagging
		createTagAndTagging := func(tagsetName string, tagTypeId int64, value string) {
			if value == "" {
				return
			}

			tagsetResp, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
				Name:      tagsetName,
				TagTypeId: tagTypeId,
			})
			if err != nil {
				log.Printf("[geodatas] failed to get/create tagset '%s': %v", tagsetName, err)
				return
			}

			tagResp, err := server.CreateTag(context.Background(), makeTagRequest(tagTypeId, tagsetResp.Id, value))
			if err != nil {
				log.Printf("[geodatas] failed to get/create tag '%s' in tagset '%s': %v", value, tagsetName, err)
				return
			}

			_, err = server.CreateTagging(context.Background(), &pb.CreateTaggingRequest{
				MediaId: mediaId,
				TagId:   tagResp.Id,
			})
			if err != nil {
				log.Printf("[geodatas] failed to create tagging media=%d tag=%d: %v", mediaId, tagResp.Id, err)
				return
			}

			log.Printf("[geodatas] tagged media=%d with %s='%s'", mediaId, tagsetName, value)
		}

		// ── Country ───────────────────────────────────────────────────────
		if v, _ := message["country"].(string); v != "" {
			createTagAndTagging("Country", 1, v)
		}

		// ── City ──────────────────────────────────────────────────────────
		if v, _ := message["city"].(string); v != "" {
			createTagAndTagging("City", 1, v)
		}

		// ── Location Type (OSM type of the location itself) ───────────────
		if v, _ := message["location_type"].(string); v != "" {
			createTagAndTagging("Location Type", 1, v)
		}

		// ── POI Categories (stored distinctly from location type) ─────────
		poisRaw, _ := message["nearby_pois"].([]interface{})
		seenCategories := make(map[string]bool)
		for _, poiRaw := range poisRaw {
			poi, ok := poiRaw.(map[string]interface{})
			if !ok {
				continue
			}
			category, _ := poi["category"].(string)
			if category == "" || seenCategories[category] {
				continue
			}
			seenCategories[category] = true
			createTagAndTagging("POI Category", 1, category)
		}

		log.Printf("[geodatas] finished processing message for media_id=%d", mediaId)
	})

	rmq.Listen("geodatas.known", func(msg amqp.Delivery) {
		// The location is already known — its tags are already in the database.
		// Nothing to do server-side; log for traceability only.
		var message map[string]interface{}
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("[geodatas.known] failed to parse message: %v", err)
			return
		}
		log.Printf("[geodatas.known] media_id=%v place_id=%v already processed, skipping",
			message["media_id"], message["place_id"])
	})
}

// listenForTaggingUpdateMessage listens on tagging_update.* and, for
// geodata-related tagsets (Country, City, Location Type, POI Category),
// forwards the update as an update_geodatas.* message so the geodata plugin
// can refresh its local database accordingly.
//
// Message received on tagging_update.<tagsetName>:
//
//	{"OldName": "...", "NewName": "...", "MediaId": "42"}
//
// Message published on update_geodatas.<tagsetName>:
//
//	{"place_id": null, "fields": {"<field>": "<NewName>"}, "media_id": "42"}
//
// Note: place_id is not available from the tagging update alone, so the
// geodata plugin is expected to resolve it from media_id when place_id is null.
func listenForTaggingUpdateMessage(server *DataLoaderServer) {
	geodataTagsets := map[string]string{
		"Country":       "country",
		"City":          "city",
		"Location Type": "location_type",
		"POI Category":  "poi_category",
	}

	rmq.Listen("tagging_update.*", func(msg amqp.Delivery) {
		log.Printf("[tagging_update] received message: %s (key=%s)", msg.Body, msg.RoutingKey)

		// Extract the tagset name from the routing key: tagging_update.<tagsetName>
		parts := strings.SplitN(msg.RoutingKey, ".", 2)
		if len(parts) < 2 {
			log.Printf("[tagging_update] invalid routing key: %s", msg.RoutingKey)
			return
		}
		tagsetName := parts[1]

		// Check if this tagset is geodata-related
		fieldName, isGeodata := geodataTagsets[tagsetName]
		if !isGeodata {
			log.Printf("[tagging_update] tagset '%s' is not geodata-related, skipping", tagsetName)
			return
		}

		var message map[string]interface{}
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("[tagging_update] failed to parse message: %v", err)
			return
		}

		newName, _ := message["NewName"].(string)
		mediaId, _ := message["MediaId"].(string)

		if newName == "" || mediaId == "" {
			log.Printf("[tagging_update] missing NewName or MediaId in message")
			return
		}

		// Build the update_geodatas message
		fields := map[string]string{fieldName: newName}
		fieldsJSON, err := json.Marshal(fields)
		if err != nil {
			log.Printf("[tagging_update] failed to marshal fields: %v", err)
			return
		}

		body := fmt.Sprintf(`{
			"place_id": null,
			"fields": %s,
			"media_id": "%s"
		}`, string(fieldsJSON), mediaId)

		rmq.PublishMessage(prod, body, fmt.Sprintf("update_geodatas.%s", tagsetName))
		log.Printf("[tagging_update] forwarded to update_geodatas.%s for media_id=%s", tagsetName, mediaId)
	})
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

	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()

	go listenForTaggingMessage(server)

	go listenForHierarchyMessage(server)

	go listenForGeodatasMessage(server)

	go listenForTaggingUpdateMessage(server)

	// Create a TCP listener for the gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", sv_host, sv_port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create and register the implementation of the gRPC server
	grpc_server := grpc.NewServer()
	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(grpc_server, healthcheck)
	pb.RegisterDataLoaderServer(grpc_server, server)
	log.Println("gRPC server listening on port 50051")
	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	healthcheck.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}
