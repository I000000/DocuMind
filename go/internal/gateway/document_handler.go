package gateway

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	documentv1 "github.com/I000000/DocuMind/gen/documind/document/v1"
	"github.com/I000000/DocuMind/internal/httperr"
)

const defaultPageSize = 20
const maxPageSize = 100

// DocumentHandler — HTTP-хендлеры gateway, которые вызывают
// document-service по gRPC.
type DocumentHandler struct {
	client *DocumentClient
	logger *zap.Logger
}

func NewDocumentHandler(client *DocumentClient, logger *zap.Logger) *DocumentHandler {
	return &DocumentHandler{
		client: client,
		logger: logger.With(zap.String("component", "gateway_document_handler")),
	}
}

// Get — GET /api/v1/documents/:id
func (h *DocumentHandler) Get(c *gin.Context) {
	token := extractBearerToken(c)
	doc, err := h.client.GetDocument(c.Request.Context(), token, c.Param("id"))
	if err != nil {
		h.respondGRPCError(c, err)
		return
	}
	c.JSON(http.StatusOK, documentToJSON(doc))
}

// List — GET /api/v1/documents?page_size=20
func (h *DocumentHandler) List(c *gin.Context) {
	token := extractBearerToken(c)
	pageSize := parsePageSize(c.Query("page_size"))

	docs, total, err := h.client.ListDocuments(c.Request.Context(), token, pageSize)
	if err != nil {
		h.respondGRPCError(c, err)
		return
	}

	out := make([]map[string]any, len(docs))
	for i, d := range docs {
		out[i] = documentToJSON(d)
	}

	c.JSON(http.StatusOK, gin.H{
		"documents":   out,
		"total_count": total,
	})
}

// Delete — DELETE /api/v1/documents/:id
func (h *DocumentHandler) Delete(c *gin.Context) {
	token := extractBearerToken(c)
	if err := h.client.DeleteDocument(c.Request.Context(), token, c.Param("id")); err != nil {
		h.respondGRPCError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *DocumentHandler) respondGRPCError(c *gin.Context, err error) {
	status, message := httperr.FromGRPC(err)
	h.logger.Warn("grpc_call_failed",
		zap.String("path", c.FullPath()),
		zap.Int("status", status),
		zap.Error(err),
	)
	c.JSON(status, gin.H{"error": message})
}

// extractBearerToken достаёт токен из Authorization header.
func extractBearerToken(c *gin.Context) string {
	header := c.GetHeader("Authorization")
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func parsePageSize(raw string) int {
	if raw == "" {
		return defaultPageSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultPageSize
	}
	if n > maxPageSize {
		return maxPageSize
	}
	return n
}

// documentToJSON конвертирует proto-структуру в JSON-совместимую map.
// Явный маппинг вместо сериализации proto напрямую — чтобы контролировать
// имена полей и форматы (snake_case вместо camelCase).
func documentToJSON(d *documentv1.Document) map[string]any {
	out := map[string]any{
		"id":           d.Id,
		"title":        d.Title,
		"content_type": d.ContentType,
		"size_bytes":   d.SizeBytes,
		"status":       statusToString(d.Status),
	}
	if d.CreatedAt != nil {
		out["created_at"] = d.CreatedAt.AsTime()
	}
	if d.UpdatedAt != nil {
		out["updated_at"] = d.UpdatedAt.AsTime()
	}
	if d.ErrorMessage != "" {
		out["error_message"] = d.ErrorMessage
	}
	return out
}

func statusToString(s documentv1.DocumentStatus) string {
	switch s {
	case documentv1.DocumentStatus_DOCUMENT_STATUS_PENDING:
		return "pending"
	case documentv1.DocumentStatus_DOCUMENT_STATUS_PROCESSING:
		return "processing"
	case documentv1.DocumentStatus_DOCUMENT_STATUS_READY:
		return "ready"
	case documentv1.DocumentStatus_DOCUMENT_STATUS_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}
