package domain

import (
	"context"
	"errors"
	"time"
)

// DocumentCreator создаёт новые документы.
type DocumentCreator interface {
	// CreateWithOutbox атомарно создаёт документ и событие в outbox.
	CreateWithOutbox(ctx context.Context, input NewDocumentInput, event OutboxEvent) error
}

// DocumentReader читает документы.
type DocumentReader interface {
	GetByID(ctx context.Context, id string) (Document, error)

	// List возвращает страницу документов. status — опциональный фильтр.
	List(ctx context.Context, limit, offset int, status string) ([]Document, int, error)
}

// DocumentWriter изменяет документы.
type DocumentWriter interface {
	// UpdateStatus меняет статус и сообщение об ошибке. errorMessage может быть nil.
	UpdateStatus(ctx context.Context, id string, status DocumentStatus, errorMessage *string) error

	Delete(ctx context.Context, id string) error
}

// DocumentRepository — полный набор операций. Хендлеры обычно используют
// узкие интерфейсы выше, композиция нужна сервисному слою.
type DocumentRepository interface {
	DocumentCreator
	DocumentReader
	DocumentWriter
}

// OutboxFetcher читает необработанные события из outbox.
type OutboxFetcher interface {
	// FetchPending использует FOR UPDATE SKIP LOCKED — параллельные воркеры не конфликтуют.
	FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)
}

// OutboxMarker обновляет статус событий.
type OutboxMarker interface {
	MarkProcessed(ctx context.Context, id int64) error

	// MarkFailed возвращает событие в pending с next_retry_at для backoff.
	MarkFailed(ctx context.Context, id int64, errMsg string, nextRetryAt time.Time) error

	// MarkDead помечает событие как окончательно провалившееся после исчерпания retry.
	MarkDead(ctx context.Context, id int64, errMsg string) error
}

// OutboxRepository — полный набор операций с outbox.
type OutboxRepository interface {
	OutboxFetcher
	OutboxMarker
}

// OutboxEvent — событие для записи в outbox при создании документа.
type OutboxEvent struct {
	EventID     string
	EventType   string
	Payload     []byte // JSON-сериализованное тело события
	TraceParent string // W3C traceparent для распределённой трассировки
	RequestID   string
}

// OutboxRecord — запись из таблицы outbox, прочитанная воркером.
type OutboxRecord struct {
	ID          int64
	EventID     string
	EventType   string
	Payload     []byte
	RetryCount  int
	TraceParent string
	RequestID   string
}

// Domain-ошибки, не зависящие от инфраструктуры.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)
