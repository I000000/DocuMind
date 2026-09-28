package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	App       AppConfig
	Postgres  PostgresConfig
	Redis     RedisConfig
	OIDC      OIDCConfig
	RateLimit RateLimitConfig
	Kafka     KafkaConfig
	MinIO     MinIOConfig
	GRPC      GRPCConfig
	OTel      OTelConfig
	Document  DocumentConfig
}

type AppConfig struct {
	Env        string `env:"APP_ENV" envDefault:"development"`
	LogLevel   string `env:"LOG_LEVEL" envDefault:"info"`
	ServerPort string `env:"SERVER_PORT" envDefault:"8080"`
	PprofPort  string `env:"PPROF_PORT" envDefault:"6060"`
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

type OIDCConfig struct {
	IssuerURL    string        `env:"OIDC_ISSUER_URL,required"`
	ClientID     string        `env:"OIDC_CLIENT_ID,required"`
	Audience     string        `env:"OIDC_AUDIENCE,required"`
	JWKSCacheTTL time.Duration `env:"OIDC_JWKS_CACHE_TTL" envDefault:"15m"`
}

type RateLimitConfig struct {
	RPS    int           `env:"RATE_LIMIT_RPS" envDefault:"50"`
	Burst  int           `env:"RATE_LIMIT_BURST" envDefault:"100"`
	Window time.Duration `env:"RATE_LIMIT_WINDOW" envDefault:"1s"`
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

type GRPCConfig struct {
	SearchServiceAddr   string `env:"SEARCH_SERVICE_GRPC_ADDR" envDefault:"localhost:50051"`
	DocumentServiceAddr string `env:"DOCUMENT_SERVICE_GRPC_ADDR" envDefault:"localhost:9090"`
}

type OTelConfig struct {
	Endpoint     string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://localhost:4318"`
	ServiceName  string  `env:"OTEL_SERVICE_NAME" envDefault:"documind-gateway"`
	SamplerRatio float64 `env:"OTEL_SAMPLER_RATIO" envDefault:"1.0"`
}

type DocumentConfig struct {
	GRPCPort       string `env:"DOCUMENT_GRPC_PORT" envDefault:"9090"`
	HTTPPort       string `env:"DOCUMENT_HTTP_PORT" envDefault:"8082"`
	MaxUploadBytes int64  `env:"DOCUMENT_MAX_UPLOAD_BYTES" envDefault:"52428800"`
}

// Load читает конфиг из env и валидирует его.
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
		if c.OIDC.IssuerURL == "" || c.OIDC.Audience == "" {
			return fmt.Errorf("OIDC settings are required in production")
		}
	}
	if c.RateLimit.RPS <= 0 || c.RateLimit.Burst <= 0 {
		return fmt.Errorf("rate limit values must be positive")
	}
	if c.OTel.SamplerRatio < 0 || c.OTel.SamplerRatio > 1 {
		return fmt.Errorf("OTEL_SAMPLER_RATIO must be in [0,1]")
	}
	return nil
}
