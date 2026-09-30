package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/document/domain"
)

// PostgresRepository — реализация domain.DocumentRepository поверх PostgreSQL.
type PostgresRepository struct {
	db     *sqlx.DB
	logger *zap.Logger
}

// Compile-time проверки: реализует ли PostgresRepository нужные интерфейсы.
var (
	_ domain.DocumentCreator    = (*PostgresRepository)(nil)
	_ domain.DocumentReader     = (*PostgresRepository)(nil)
	_ domain.DocumentWriter     = (*PostgresRepository)(nil)
	_ domain.DocumentRepository = (*PostgresRepository)(nil)
)

func NewPostgresRepository(db *sqlx.DB, logger *zap.Logger) *PostgresRepository {
	return &PostgresRepository{
		db:     db,
		logger: logger.With(zap.String("component", "document_repository")),
	}
}

// CreateWithOutbox атомарно создаёт документ и событие в outbox.
// Обе записи в одной транзакции: если падает одна — откатывается другая.
func (r *PostgresRepository) CreateWithOutbox(
	ctx context.Context,
	input domain.NewDocumentInput,
	event domain.OutboxEvent,
) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO documents (
			id, title, content_type, size_bytes, status, source,
			uploaded_by_user_id, uploaded_by_email
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		input.ID,
		input.Title,
		input.ContentType,
		input.SizeBytes,
		domain.StatusPending,
		input.Source,
		input.UploadedByUserID,
		input.UploadedByEmail,
	)
	if err != nil {
		return fmt.Errorf("insert document: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (event_id, event_type, payload, trace_parent, request_id, status)
		VALUES ($1, $2, $3, $4, $5, 'pending')
	`,
		event.EventID,
		event.EventType,
		event.Payload,
		event.TraceParent,
		event.RequestID,
	)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	r.logger.Info("document_created_with_outbox",
		zap.String("document_id", input.ID),
		zap.String("event_id", event.EventID),
	)
	return nil
}

// GetByID возвращает документ или domain.ErrNotFound.
func (r *PostgresRepository) GetByID(ctx context.Context, id string) (domain.Document, error) {
	var doc domain.Document
	err := r.db.GetContext(ctx, &doc, `
		SELECT id, title, content_type, size_bytes, status, error_message, source,
		       uploaded_by_user_id, uploaded_by_email, created_at, updated_at
		FROM documents WHERE id = $1
	`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Document{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Document{}, fmt.Errorf("get document: %w", err)
	}
	return doc, nil
}

// List возвращает страницу документов и общее количество.
// status — опциональный фильтр, пустая строка = без фильтра.
func (r *PostgresRepository) List(
	ctx context.Context,
	limit int,
	offset int,
	status string,
) ([]domain.Document, int, error) {
	var (
		docs  []domain.Document
		total int
	)

	whereClause := ""
	args := []any{}
	if status != "" {
		whereClause = "WHERE status = $1"
		args = append(args, status)
	}

	countQuery := "SELECT COUNT(*) FROM documents " + whereClause
	if err := r.db.GetContext(ctx, &total, countQuery, args...); err != nil {
		return nil, 0, fmt.Errorf("count: %w", err)
	}

	// Нумерация $1, $2 зависит от того, есть ли фильтр — считаем позиции динамически.
	listQuery := fmt.Sprintf(`
		SELECT id, title, content_type, size_bytes, status, error_message, source,
		       uploaded_by_user_id, uploaded_by_email, created_at, updated_at
		FROM documents %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, len(args)+1, len(args)+2)

	args = append(args, limit, offset)
	if err := r.db.SelectContext(ctx, &docs, listQuery, args...); err != nil {
		return nil, 0, fmt.Errorf("list: %w", err)
	}

	return docs, total, nil
}

// UpdateStatus меняет статус документа. errorMessage может быть nil.
func (r *PostgresRepository) UpdateStatus(
	ctx context.Context,
	id string,
	status domain.DocumentStatus,
	errorMessage *string,
) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE documents
		SET status = $1, error_message = $2, updated_at = NOW()
		WHERE id = $3
	`, status, errorMessage, id)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

// Delete удаляет документ и каскадно его чанки или возвращает domain.ErrNotFound.
func (r *PostgresRepository) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, "DELETE FROM documents WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return domain.ErrNotFound
	}
	return nil
}
