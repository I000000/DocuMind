package service

import "errors"

// Sentinel-ошибки сервисного слоя. Хендлер маппит их в HTTP-коды.
var (
	ErrInvalidInput  = errors.New("invalid input")
	ErrUploadFailed  = errors.New("storage upload failed")
	ErrPersistFailed = errors.New("persist failed")
)
