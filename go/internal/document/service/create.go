package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/document/domain"
	"github.com/I000000/DocuMind/internal/document/events"
	"github.com/I000000/DocuMind/internal/minio"
	"github.com/I000000/DocuMind/internal/tracing"
)

// CreateInput — входные данные для создания документа.
type CreateInput struct {
	Title       string
	ContentType string
	Size        int64
	Reader      io.Reader
	UserID      string
	UserEmail   string
	RequestID   string
}

// CreateOutput — результат создания.
type CreateOutput struct {
	ID     string
	Status domain.DocumentStatus
}

// Creator создаёт документы.
type Creator interface {
	Create(ctx context.Context, input CreateInput) (CreateOutput, error)
}

// CreatorService — реализация Creator.
//
// Пайплайн:
//  1. Генерирует UUID и ключ объекта в MinIO.
//  2. Загружает файл в MinIO.
//  3. Формирует событие document.uploaded с traceparent из ctx.
//  4. Атомарно пишет документ и outbox-событие в БД.
//
// Если шаг 4 падает — best-effort удаляет загруженный файл (компенсация).
// Orphan-файлы в MinIO безопасны: они не попадут в БД и не будут обработаны.
type CreatorService struct {
	repo    domain.DocumentCreator
	storage *minio.Client
	logger  *zap.Logger
}

var _ Creator = (*CreatorService)(nil)

func NewCreatorService(
	repo domain.DocumentCreator,
	storage *minio.Client,
	logger *zap.Logger,
) *CreatorService {
	return &CreatorService{
		repo:    repo,
		storage: storage,
		logger:  logger.With(zap.String("component", "creator_service")),
	}
}

func (s *CreatorService) Create(ctx context.Context, input CreateInput) (CreateOutput, error) {
	if input.Title == "" {
		return CreateOutput{}, fmt.Errorf("%w: title is required", ErrInvalidInput)
	}
	if input.Size <= 0 {
		return CreateOutput{}, fmt.Errorf("%w: file is empty", ErrInvalidInput)
	}

	documentID := uuid.NewString()
	eventID := uuid.NewString()
	objectKey := documentID + filepath.Ext(input.Title)

	if err := s.storage.Upload(ctx, objectKey, input.Reader, input.Size, input.ContentType); err != nil {
		return CreateOutput{}, fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}

	traceParent := tracing.InjectToCarrier(ctx)["traceparent"]

	payload, err := json.Marshal(events.DocumentUploaded{
		EventID:    eventID,
		EventType:  events.EventTypeDocumentUploaded,
		Version:    events.EventVersionV1,
		OccurredAt: time.Now().UTC(),
		RequestID:  input.RequestID,
		Payload: events.DocumentUploadedPayload{
			DocumentID:  documentID,
			Title:       input.Title,
			ContentType: input.ContentType,
			SizeBytes:   input.Size,
			Storage: events.StorageInfo{
				Provider: "minio",
				Bucket:   s.storage.Bucket(),
				Key:      objectKey,
			},
			UploadedBy: uploadedBy(input.UserID, input.UserEmail),
		},
	})
	if err != nil {
		_ = s.storage.Delete(ctx, objectKey)
		return CreateOutput{}, fmt.Errorf("marshal event: %w", err)
	}

	err = s.repo.CreateWithOutbox(ctx, domain.NewDocumentInput{
		ID:               documentID,
		Title:            input.Title,
		ContentType:      input.ContentType,
		SizeBytes:        input.Size,
		Source:           objectKey,
		UploadedByUserID: stringPtr(input.UserID),
		UploadedByEmail:  stringPtr(input.UserEmail),
	}, domain.OutboxEvent{
		EventID:     eventID,
		EventType:   events.EventTypeDocumentUploaded,
		Payload:     payload,
		TraceParent: traceParent,
		RequestID:   input.RequestID,
	})
	if err != nil {
		// Компенсация: удаляем загруженный объект. Best-effort — orphan-файлы
		// в MinIO не наносят вреда и могут быть очищены отдельным job'ом.
		if delErr := s.storage.Delete(ctx, objectKey); delErr != nil {
			s.logger.Warn("compensation_failed",
				zap.String("document_id", documentID),
				zap.String("object_key", objectKey),
				zap.Error(delErr),
			)
		}
		return CreateOutput{}, fmt.Errorf("%w: %v", ErrPersistFailed, err)
	}

	s.logger.Info("document_created",
		zap.String("document_id", documentID),
		zap.String("event_id", eventID),
		zap.Int64("size_bytes", input.Size),
	)

	return CreateOutput{ID: documentID, Status: domain.StatusPending}, nil
}

func uploadedBy(userID, email string) *events.UploadedBy {
	if userID == "" && email == "" {
		return nil
	}
	return &events.UploadedBy{UserID: userID, Email: email}
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
