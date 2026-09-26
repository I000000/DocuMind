package middleware

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ctxKey — типизированный ключ контекста.
type ctxKey string

const (
	RequestIDKey    ctxKey = "request_id"
	RequestIDHeader        = "X-Request-ID"
)

// RequestID извлекает X-Request-ID из заголовка или генерирует новый.
// Кладёт его в context и в gin.Context, а также возвращает в response header.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(RequestIDHeader)
		if requestID == "" {
			requestID = uuid.NewString()
		}

		ctx := context.WithValue(c.Request.Context(), RequestIDKey, requestID)
		c.Request = c.Request.WithContext(ctx)
		c.Set(string(RequestIDKey), requestID)
		c.Header(RequestIDHeader, requestID)

		c.Next()
	}
}

// GetRequestID возвращает request ID из контекста или пустую строку.
func GetRequestID(ctx context.Context) string {
	if v, ok := ctx.Value(RequestIDKey).(string); ok {
		return v
	}
	return ""
}
