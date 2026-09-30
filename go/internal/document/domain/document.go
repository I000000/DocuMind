package domain

import (
	"time"
)

// DocumentStatus — статус обработки документа.
type DocumentStatus string

const (
	StatusPending    DocumentStatus = "pending"
	StatusProcessing DocumentStatus = "processing"
	StatusReady      DocumentStatus = "ready"
	StatusFailed     DocumentStatus = "failed"
)

// Document — метаданные документа.
type Document struct {
	ID               string         `db:"id"`
	Title            string         `db:"title"`
	ContentType      string         `db:"content_type"`
	SizeBytes        int64          `db:"size_bytes"`
	Status           DocumentStatus `db:"status"`
	ErrorMessage     *string        `db:"error_message"`
	Source           *string        `db:"source"`
	UploadedByUserID *string        `db:"uploaded_by_user_id"`
	UploadedByEmail  *string        `db:"uploaded_by_email"`
	CreatedAt        time.Time      `db:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at"`
}

// NewDocumentInput — входные данные для создания документа.
type NewDocumentInput struct {
	ID               string
	Title            string
	ContentType      string
	SizeBytes        int64
	Source           string
	UploadedByUserID *string
	UploadedByEmail  *string
}
