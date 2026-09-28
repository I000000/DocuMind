package minio

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

// Client — обёртка над minio.Client с логированием и идемпотентным созданием bucket.
type Client struct {
	client *minio.Client
	bucket string
	logger *zap.Logger
}

// Options — параметры подключения к MinIO.
type Options struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

// NewClient создаёт MinIO-клиент и проверяет соединение с сервером.
// Bucket создаётся отдельно — через EnsureBucket
func NewClient(ctx context.Context, opts Options, logger *zap.Logger) (*Client, error) {
	cli, err := minio.New(opts.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKey, opts.SecretKey, ""),
		Secure: opts.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	// Проверяем, что сервер отвечает — fail fast.
	exists, err := cli.BucketExists(ctx, opts.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}

	logger.Info("minio_client_ready",
		zap.String("endpoint", opts.Endpoint),
		zap.String("bucket", opts.Bucket),
		zap.Bool("bucket_exists", exists),
	)

	return &Client{
		client: cli,
		bucket: opts.Bucket,
		logger: logger,
	}, nil
}

// EnsureBucket создаёт bucket, если его нет. Идемпотентно.
func (c *Client) EnsureBucket(ctx context.Context) error {
	exists, err := c.client.BucketExists(ctx, c.bucket)
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	if exists {
		return nil
	}

	if err := c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("make bucket: %w", err)
	}

	c.logger.Info("minio_bucket_created", zap.String("bucket", c.bucket))
	return nil
}

// Upload загружает данные в bucket. Возвращает ключ объекта.
func (c *Client) Upload(
	ctx context.Context,
	objectKey string,
	reader io.Reader,
	size int64,
	contentType string,
) error {
	_, err := c.client.PutObject(ctx, c.bucket, objectKey, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	c.logger.Info("minio_uploaded",
		zap.String("bucket", c.bucket),
		zap.String("key", objectKey),
		zap.Int64("size", size),
	)
	return nil
}

// Delete удаляет объект. Не возвращает ошибку, если объекта нет.
func (c *Client) Delete(ctx context.Context, objectKey string) error {
	err := c.client.RemoveObject(ctx, c.bucket, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("remove object: %w", err)
	}

	c.logger.Info("minio_deleted",
		zap.String("bucket", c.bucket),
		zap.String("key", objectKey),
	)
	return nil
}

// PresignedURL генерирует временную ссылку на скачивание объекта.
func (c *Client) PresignedURL(ctx context.Context, objectKey string, ttl time.Duration) (string, error) {
	url, err := c.client.PresignedGetObject(ctx, c.bucket, objectKey, ttl, nil)
	if err != nil {
		return "", fmt.Errorf("presign: %w", err)
	}
	return url.String(), nil
}

// Bucket возвращает имя bucket — для событий Kafka и метаданных.
func (c *Client) Bucket() string {
	return c.bucket
}
