package main

import (
	"context"
	"database/sql"
	"fmt"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
)

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
	// If we are trying to add a rootnode, the operations are a bit different
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
			// If node nonè-existent we add it, retrieve its ID and update the hierarchy's rootnode_id value
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
