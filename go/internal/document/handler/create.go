package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/auth"
	"github.com/I000000/DocuMind/internal/document/service"
	"github.com/I000000/DocuMind/internal/middleware"
)

// Create — POST /api/v1/documents
//
// Принимает multipart/form-data с полем "file". Валидирует размер,
// открывает поток и передаёт в сервис. Отвечает 201 Created с ID документа.
//
// Ошибки валидации запроса (нет файла, превышен размер) обрабатываются
// здесь же — они не domain-specific. Domain-ошибки из сервиса уходят
// в errMapper, который сам решает статус и формат ответа.
func (h *Handler) Create(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "file is required (multipart field 'file')",
		})
		return
	}

	if file.Size > h.maxUploadBytes {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"error": "file exceeds maximum allowed size",
			"max":   h.maxUploadBytes,
		})
		return
	}

	src, err := file.Open()
	if err != nil {
		h.logger.Error("open_upload", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer func() { _ = src.Close() }()

	// Аутентификация опциональна: если есть auth middleware, берём User из
	// контекста. Если нет — загрузка анонимная (для dev и тестов).
	var userID, userEmail string
	if u, ok := auth.UserFromContext(c.Request.Context()); ok {
		userID = u.Subject
		userEmail = u.Email
	}

	result, err := h.creator.Create(c.Request.Context(), service.CreateInput{
		Title:       file.Filename,
		ContentType: file.Header.Get("Content-Type"),
		Size:        file.Size,
		Reader:      src,
		UserID:      userID,
		UserEmail:   userEmail,
		RequestID:   middleware.GetRequestID(c.Request.Context()),
	})
	if err != nil {
		h.errMapper.Respond(c, err)
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"document_id": result.ID,
		"status":      result.Status,
	})
}
