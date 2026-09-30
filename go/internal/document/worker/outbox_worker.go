package worker

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/config"
	"github.com/I000000/DocuMind/internal/document/domain"
	"github.com/I000000/DocuMind/internal/kafka"
	"github.com/I000000/DocuMind/internal/tracing"
)

// OutboxWorker — фоновый публикатор событий из outbox в Kafka.
//
// Гарантии:
// - At-least-once: событие коммитится processed только после успешной отправки.
// - Exponential backoff: retry_count → next_retry_at по формуле base * 2^n (cap max).
// - Deadline: после MaxRetries событие уходит в 'dead' и требует ручного разбора.
type OutboxWorker struct {
	repo     domain.OutboxRepository
	producer *kafka.Producer
	cfg      config.OutboxConfig
	logger   *zap.Logger
}

func NewOutboxWorker(
	repo domain.OutboxRepository,
	producer *kafka.Producer,
	cfg config.OutboxConfig,
	logger *zap.Logger,
) *OutboxWorker {
	return &OutboxWorker{
		repo:     repo,
		producer: producer,
		cfg:      cfg,
		logger:   logger.With(zap.String("component", "outbox_worker")),
	}
}

// Run — основной цикл. Останавливается по отмене ctx.
func (w *OutboxWorker) Run(ctx context.Context) error {
	w.logger.Info("outbox_worker_started",
		zap.Duration("poll_interval", w.cfg.PollInterval),
		zap.Int("batch_size", w.cfg.BatchSize),
	)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("outbox_worker_stopped")
			return ctx.Err()
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

// processBatch читает пачку pending-событий и обрабатывает каждое.
// Ошибки обработки одного события не прерывают батч.
func (w *OutboxWorker) processBatch(ctx context.Context) {
	events, err := w.repo.FetchPending(ctx, w.cfg.BatchSize)
	if err != nil {
		w.logger.Error("fetch_pending_failed", zap.Error(err))
		return
	}
	if len(events) == 0 {
		return
	}

	w.logger.Debug("batch_fetched", zap.Int("count", len(events)))

	for _, evt := range events {
		w.processEvent(ctx, evt)
	}
}

// processEvent публикует одно событие в Kafka или помечает его failed/dead.
func (w *OutboxWorker) processEvent(ctx context.Context, evt domain.OutboxRecord) {
	// Восстанавливаем trace context из сохранённого traceparent —
	// span из HTTP-запроса продолжается в Kafka producer'е.
	carrier := map[string]string{}
	if evt.TraceParent != "" {
		carrier["traceparent"] = evt.TraceParent
	}
	pubCtx := tracing.ExtractFromCarrier(ctx, carrier)

	headers := map[string]string{
		"X-Event-Type": evt.EventType,
	}
	if evt.RequestID != "" {
		headers["X-Request-ID"] = evt.RequestID
	}

	_, _, err := w.producer.Publish(
		pubCtx,
		evt.EventID,
		json.RawMessage(evt.Payload),
		headers,
	)
	if err != nil {
		w.handlePublishFailure(ctx, evt, err)
		return
	}

	if err := w.repo.MarkProcessed(ctx, evt.ID); err != nil {
		// Событие уже в Kafka, но статус не обновился. При следующем цикле
		// Kafka получит дубликат — консьюмеры идемпотентны по event_id.
		w.logger.Error("mark_processed_failed",
			zap.Int64("id", evt.ID),
			zap.String("event_id", evt.EventID),
			zap.Error(err),
		)
		return
	}

	w.logger.Info("event_published",
		zap.Int64("id", evt.ID),
		zap.String("event_id", evt.EventID),
		zap.String("event_type", evt.EventType),
	)
}

// handlePublishFailure решает, что делать с событием: retry или dead.
func (w *OutboxWorker) handlePublishFailure(ctx context.Context, evt domain.OutboxRecord, publishErr error) {
	if evt.RetryCount >= w.cfg.MaxRetries {
		if err := w.repo.MarkDead(ctx, evt.ID, publishErr.Error()); err != nil {
			w.logger.Error("mark_dead_failed", zap.Int64("id", evt.ID), zap.Error(err))
		}
		return
	}

	nextRetry := w.calculateNextRetry(evt.RetryCount)

	if err := w.repo.MarkFailed(ctx, evt.ID, publishErr.Error(), nextRetry); err != nil {
		w.logger.Error("mark_failed_failed", zap.Int64("id", evt.ID), zap.Error(err))
		return
	}

	w.logger.Warn("event_publish_failed_retry_scheduled",
		zap.Int64("id", evt.ID),
		zap.String("event_id", evt.EventID),
		zap.Int("attempt", evt.RetryCount+1),
		zap.Duration("next_retry_in", time.Until(nextRetry)),
		zap.Error(publishErr),
	)
}

// calculateNextRetry возвращает момент следующей попытки по формуле base * 2^n.
// Ограничивается BackoffMax. Цикл вместо битового сдвига — защита от overflow.
func (w *OutboxWorker) calculateNextRetry(retryCount int) time.Time {
	delay := w.cfg.BackoffBase
	for i := 0; i < retryCount; i++ {
		delay *= 2
		if delay >= w.cfg.BackoffMax {
			delay = w.cfg.BackoffMax
			break
		}
	}
	return time.Now().Add(delay)
}
