package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/document/domain"
)

// OutboxPostgresRepository — реализация domain.OutboxRepository поверх PostgreSQL.
type OutboxPostgresRepository struct {
	db     *sqlx.DB
	logger *zap.Logger
}

// Compile-time проверки: реализует ли OutboxPostgresRepository нужные интерфейсы.
var (
	_ domain.OutboxFetcher    = (*OutboxPostgresRepository)(nil)
	_ domain.OutboxMarker     = (*OutboxPostgresRepository)(nil)
	_ domain.OutboxRepository = (*OutboxPostgresRepository)(nil)
)

func NewOutboxPostgresRepository(db *sqlx.DB, logger *zap.Logger) *OutboxPostgresRepository {
	return &OutboxPostgresRepository{
		db:     db,
		logger: logger.With(zap.String("component", "outbox_repository")),
	}
}

// FetchPending читает пачку pending-событий и помечает их processing.
// FOR UPDATE SKIP LOCKED — параллельные воркеры не блокируют друг друга.
func (r *OutboxPostgresRepository) FetchPending(
	ctx context.Context,
	limit int,
) ([]domain.OutboxRecord, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var rows []struct {
		ID          int64  `db:"id"`
		EventID     string `db:"event_id"`
		EventType   string `db:"event_type"`
		Payload     []byte `db:"payload"`
		RetryCount  int    `db:"retry_count"`
		TraceParent string `db:"trace_parent"`
		RequestID   string `db:"request_id"`
	}

	err = tx.SelectContext(ctx, &rows, `
		SELECT id, event_id, event_type, payload, retry_count,
		       COALESCE(trace_parent, '') AS trace_parent,
		       COALESCE(request_id, '') AS request_id
		FROM outbox
		WHERE status = 'pending'
		  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending: %w", err)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	// Помечаем processing сразу — иначе другой воркер подхватит те же строки
	// после того, как мы отпустим блокировку транзакцией.
	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE outbox SET status = 'processing' WHERE id = ANY($1)
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("mark processing: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	result := make([]domain.OutboxRecord, len(rows))
	for i, row := range rows {
		result[i] = domain.OutboxRecord{
			ID:          row.ID,
			EventID:     row.EventID,
			EventType:   row.EventType,
			Payload:     row.Payload,
			RetryCount:  row.RetryCount,
			TraceParent: row.TraceParent,
			RequestID:   row.RequestID,
		}
	}
	return result, nil
}

// MarkProcessed помечает событие как успешно отправленное в Kafka.
func (r *OutboxPostgresRepository) MarkProcessed(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox
		SET status = 'processed', processed_at = NOW()
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	return nil
}

// MarkFailed возвращает событие в pending с новой датой retry.
// retry_count инкрементится атомарно в SQL, чтобы параллельные вызовы не потеряли попытку.
func (r *OutboxPostgresRepository) MarkFailed(
	ctx context.Context,
	id int64,
	errMsg string,
	nextRetryAt time.Time,
) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox
		SET status = 'pending',
		    retry_count = retry_count + 1,
		    last_error = $1,
		    next_retry_at = $2
		WHERE id = $3
	`, errMsg, nextRetryAt, id)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}
