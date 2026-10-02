package handler

import (
	"net/http"

	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/document/service"
	"github.com/I000000/DocuMind/internal/httperr"
)

// NewErrorMapper создаёт Mapper с правилами маппинга доменных ошибок
// document-сервиса в HTTP-ответы.
//
// Регистрируется один раз при старте сервиса. Хендлеры передают ошибку
// в Mapper, который сам решает статус и формат ответа.
func NewErrorMapper(logger *zap.Logger) *httperr.Mapper {
	return httperr.New(logger,
		httperr.Rule{
			Err:     service.ErrInvalidInput,
			Status:  http.StatusBadRequest,
			Message: "invalid input",
		},
		httperr.Rule{
			Err:     service.ErrUploadFailed,
			Status:  http.StatusServiceUnavailable,
			Message: "storage is temporarily unavailable",
		},
		httperr.Rule{
			Err:     service.ErrPersistFailed,
			Status:  http.StatusInternalServerError,
			Message: "failed to persist document",
		},
	)
}
