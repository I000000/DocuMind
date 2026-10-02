package handler

import (
	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/document/service"
	"github.com/I000000/DocuMind/internal/httperr"
)

// Handler содержит зависимости HTTP-слоя Document Service.
//
// Интерфейсы принимаются узкие (ISP).
type Handler struct {
	creator        service.Creator
	maxUploadBytes int64
	errMapper      *httperr.Mapper
	logger         *zap.Logger
}

func New(
	creator service.Creator,
	maxUploadBytes int64,
	errMapper *httperr.Mapper,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		creator:        creator,
		maxUploadBytes: maxUploadBytes,
		errMapper:      errMapper,
		logger:         logger.With(zap.String("component", "document_handler")),
	}
}
