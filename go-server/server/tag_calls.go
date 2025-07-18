package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
)

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
