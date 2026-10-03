package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config — корневая конфигурация Go-сервисов DocuMind.
//
// Разделена по принципу "Servers vs Clients":
//   - Server: порты, которые слушает сам сервис.
//   - Clients: адреса других сервисов, которые он вызывает.
//
// Каждый сервис читает свой .env файл (.env.gateway, .env.document-service).
type Config struct {
	App       AppConfig
	Server    ServerConfig
	Clients   ClientsConfig
	Postgres  PostgresConfig
	Redis     RedisConfig
	Kafka     KafkaConfig
	MinIO     MinIOConfig
	OIDC      OIDCConfig
	OTel      OTelConfig
	RateLimit RateLimitConfig
	Document  DocumentConfig
	Outbox    OutboxConfig
}

// AppConfig — общие настройки, одинаковые для всех сервисов.
type AppConfig struct {
	Env      string `env:"APP_ENV" envDefault:"development"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}

// ServerConfig — порты, которые слушает этот сервис.
type ServerConfig struct {
	HTTPPort  string `env:"HTTP_PORT" envDefault:"8080"`
	GRPCPort  string `env:"GRPC_PORT" envDefault:"9090"`
	PprofPort string `env:"PPROF_PORT" envDefault:"6060"`
}

// ClientsConfig — адреса других сервисов, которые вызываем.
type ClientsConfig struct {
	DocumentServiceGRPCAddr string `env:"DOCUMENT_SERVICE_GRPC_ADDR" envDefault:"localhost:9090"`
	SearchServiceGRPCAddr   string `env:"SEARCH_SERVICE_GRPC_ADDR" envDefault:"localhost:50051"`

	// DocumentServiceURL — HTTP URL document-service для проксирования upload.
	DocumentServiceURL string `env:"DOCUMENT_SERVICE_URL" envDefault:"http://localhost:8082"`
}

type PostgresConfig struct {
	Host            string        `env:"DB_HOST,required"`
	Port            string        `env:"DB_PORT" envDefault:"5432"`
	User            string        `env:"DB_USER,required"`
	Password        string        `env:"DB_PASSWORD,required"`
	Name            string        `env:"DB_NAME,required"`
	SSLMode         string        `env:"DB_SSLMODE" envDefault:"disable"`
	MaxOpenConns    int           `env:"DB_MAX_OPEN_CONNS" envDefault:"20"`
	MaxIdleConns    int           `env:"DB_MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime time.Duration `env:"DB_CONN_MAX_LIFETIME" envDefault:"30m"`
	ConnMaxIdleTime time.Duration `env:"DB_CONN_MAX_IDLE_TIME" envDefault:"5m"`
}

// DSN собирает строку подключения для lib/pq.
func (p PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Name, p.SSLMode,
	)
}

type RedisConfig struct {
	Addr     string `env:"REDIS_ADDR,required"`
	Password string `env:"REDIS_PASSWORD"`
	DB       int    `env:"REDIS_DB" envDefault:"0"`
}

type KafkaConfig struct {
	Brokers           string `env:"KAFKA_BROKERS,required"`
	DocumentsTopic    string `env:"KAFKA_TOPIC_DOCUMENTS" envDefault:"documind.documents.v1"`
	SchemaRegistryURL string `env:"KAFKA_SCHEMA_REGISTRY_URL"`
}

type MinIOConfig struct {
	Endpoint  string `env:"MINIO_ENDPOINT,required"`
	AccessKey string `env:"MINIO_ACCESS_KEY,required"`
	SecretKey string `env:"MINIO_SECRET_KEY,required"`
	Bucket    string `env:"MINIO_BUCKET" envDefault:"documents"`
	UseSSL    bool   `env:"MINIO_USE_SSL" envDefault:"false"`
}

type OIDCConfig struct {
	IssuerURL    string        `env:"OIDC_ISSUER_URL,required"`
	ClientID     string        `env:"OIDC_CLIENT_ID,required"`
	Audience     string        `env:"OIDC_AUDIENCE,required"`
	JWKSCacheTTL time.Duration `env:"OIDC_JWKS_CACHE_TTL" envDefault:"15m"`
}

type OTelConfig struct {
	Endpoint     string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://localhost:4318"`
	ServiceName  string  `env:"OTEL_SERVICE_NAME,required"`
	SamplerRatio float64 `env:"OTEL_SAMPLER_RATIO" envDefault:"1.0"`
}

type RateLimitConfig struct {
	RPS    int           `env:"RATE_LIMIT_RPS" envDefault:"50"`
	Burst  int           `env:"RATE_LIMIT_BURST" envDefault:"100"`
	Window time.Duration `env:"RATE_LIMIT_WINDOW" envDefault:"1s"`
}

type DocumentConfig struct {
	MaxUploadBytes int64 `env:"DOCUMENT_MAX_UPLOAD_BYTES" envDefault:"52428800"`
}

type OutboxConfig struct {
	PollInterval time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"1s"`
	BatchSize    int           `env:"OUTBOX_BATCH_SIZE" envDefault:"50"`
	MaxRetries   int           `env:"OUTBOX_MAX_RETRIES" envDefault:"10"`
	BackoffBase  time.Duration `env:"OUTBOX_BACKOFF_BASE" envDefault:"1s"`
	BackoffMax   time.Duration `env:"OUTBOX_BACKOFF_MAX" envDefault:"5m"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.App.Env == "production" {
		if c.Postgres.SSLMode == "disable" {
			return fmt.Errorf("DB_SSLMODE must not be 'disable' in production")
		}
	}
	if c.Outbox.BatchSize <= 0 {
		return fmt.Errorf("OUTBOX_BATCH_SIZE must be positive")
	}
	if c.Outbox.MaxRetries <= 0 {
		return fmt.Errorf("OUTBOX_MAX_RETRIES must be positive")
	}
	if c.Outbox.BackoffMax < c.Outbox.BackoffBase {
		return fmt.Errorf("OUTBOX_BACKOFF_MAX must be >= OUTBOX_BACKOFF_BASE")
	}
	if c.OTel.SamplerRatio < 0 || c.OTel.SamplerRatio > 1 {
		return fmt.Errorf("OTEL_SAMPLER_RATIO must be in [0,1]")
	}
	return nil
}
