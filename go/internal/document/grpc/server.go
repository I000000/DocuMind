package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	documentv1 "github.com/I000000/DocuMind/gen/documind/document/v1"
	"github.com/I000000/DocuMind/internal/document/domain"
)

// Server реализует documentv1.DocumentServiceServer.
type Server struct {
	documentv1.UnimplementedDocumentServiceServer

	repo domain.DocumentRepository
}

func NewServer(repo domain.DocumentRepository) *Server {
	return &Server{repo: repo}
}

// GetDocument возвращает метаданные документа.
func (s *Server) GetDocument(
	ctx context.Context,
	req *documentv1.GetDocumentRequest,
) (*documentv1.GetDocumentResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	doc, err := s.repo.GetByID(ctx, req.Id)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "document %s not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get document: %v", err)
	}

	return &documentv1.GetDocumentResponse{
		Document: toProto(doc),
	}, nil
}

// ListDocuments возвращает постраничный список документов.
func (s *Server) ListDocuments(
	ctx context.Context,
	req *documentv1.ListDocumentsRequest,
) (*documentv1.ListDocumentsResponse, error) {
	limit := int(req.PageSize)
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	// page_token пока не реализован — используем offset=0.
	// В следующих итерациях: base64(offset) как opaque cursor.
	offset := 0

	docs, total, err := s.repo.List(ctx, limit, offset, "")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list documents: %v", err)
	}

	protoDocs := make([]*documentv1.Document, len(docs))
	for i, d := range docs {
		protoDocs[i] = toProto(d)
	}

	return &documentv1.ListDocumentsResponse{
		Documents:  protoDocs,
		TotalCount: int32(total),
	}, nil
}

// DeleteDocument удаляет документ и все его чанки.
func (s *Server) DeleteDocument(
	ctx context.Context,
	req *documentv1.DeleteDocumentRequest,
) (*documentv1.DeleteDocumentResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	err := s.repo.Delete(ctx, req.Id)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "document %s not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}

	return &documentv1.DeleteDocumentResponse{Deleted: true}, nil
}

// toProto маппит domain.Document → protobuf.
func toProto(d domain.Document) *documentv1.Document {
	out := &documentv1.Document{
		Id:          d.ID,
		Title:       d.Title,
		ContentType: d.ContentType,
		SizeBytes:   d.SizeBytes,
		Status:      toProtoStatus(d.Status),
		CreatedAt:   timestamppb.New(d.CreatedAt),
		UpdatedAt:   timestamppb.New(d.UpdatedAt),
	}
	if d.ErrorMessage != nil {
		out.ErrorMessage = *d.ErrorMessage
	}
	return out
}

func toProtoStatus(s domain.DocumentStatus) documentv1.DocumentStatus {
	switch s {
	case domain.StatusPending:
		return documentv1.DocumentStatus_DOCUMENT_STATUS_PENDING
	case domain.StatusProcessing:
		return documentv1.DocumentStatus_DOCUMENT_STATUS_PROCESSING
	case domain.StatusReady:
		return documentv1.DocumentStatus_DOCUMENT_STATUS_READY
	case domain.StatusFailed:
		return documentv1.DocumentStatus_DOCUMENT_STATUS_FAILED
	default:
		return documentv1.DocumentStatus_DOCUMENT_STATUS_UNSPECIFIED
	}
}
