package events

import "time"

// Event types, разделяемые с Python-консьюмером.
const (
	EventTypeDocumentUploaded = "document.uploaded"
	EventVersionV1            = 1
)

// DocumentUploaded — событие о загрузке документа в MinIO.
// Публикуется в топик documind.documents.v1 после успешной записи в БД.
type DocumentUploaded struct {
	EventID    string                  `json:"event_id"`
	EventType  string                  `json:"event_type"`
	Version    int                     `json:"version"`
	OccurredAt time.Time               `json:"occurred_at"`
	TraceID    string                  `json:"trace_id,omitempty"`
	RequestID  string                  `json:"request_id,omitempty"`
	Payload    DocumentUploadedPayload `json:"payload"`
}

// DocumentUploadedPayload — тело события.
type DocumentUploadedPayload struct {
	DocumentID  string      `json:"document_id"`
	Title       string      `json:"title"`
	ContentType string      `json:"content_type"`
	SizeBytes   int64       `json:"size_bytes"`
	Storage     StorageInfo `json:"storage"`
	UploadedBy  *UploadedBy `json:"uploaded_by,omitempty"`
}

// StorageInfo указывает, где лежит файл.
type StorageInfo struct {
	Provider string `json:"provider"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
}

// UploadedBy — кто загрузил. nil для анонимной загрузки.
type UploadedBy struct {
	UserID string `json:"user_id,omitempty"`
	Email  string `json:"email,omitempty"`
}
