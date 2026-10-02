package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	documentv1 "github.com/I000000/DocuMind/gen/documind/document/v1"
	"github.com/I000000/DocuMind/internal/config"
	documentgrpc "github.com/I000000/DocuMind/internal/document/grpc"
	"github.com/I000000/DocuMind/internal/document/handler"
	"github.com/I000000/DocuMind/internal/document/repository"
	"github.com/I000000/DocuMind/internal/document/service"
	"github.com/I000000/DocuMind/internal/document/worker"
	"github.com/I000000/DocuMind/internal/grpcserver"
	"github.com/I000000/DocuMind/internal/health"
	"github.com/I000000/DocuMind/internal/kafka"
	"github.com/I000000/DocuMind/internal/logger"
	"github.com/I000000/DocuMind/internal/middleware"
	"github.com/I000000/DocuMind/internal/migrations"
	"github.com/I000000/DocuMind/internal/minio"
	"github.com/I000000/DocuMind/internal/server"
	"github.com/I000000/DocuMind/internal/tracing"
)

func main() {
	_ = godotenv.Load(".env")

	_ = os.Setenv("OTEL_SERVICE_NAME", "documind-document-service")

	// ---------- Config ----------
	cfg, err := config.Load()
	if err != nil {
		panic("config error: " + err.Error())
	}

	// ---------- Logger ----------
	log, err := logger.New(cfg.App.Env, cfg.App.LogLevel)
	if err != nil {
		panic("logger error: " + err.Error())
	}
	defer func() { _ = log.Sync() }()

	log.Info("starting document-service",
		zap.String("env", cfg.App.Env),
		zap.String("version", "dev"),
	)

	// ---------- Context с отменой по сигналу ----------
	rootCtx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- Tracing ----------
	shutdownTracer, err := tracing.InitTracer(rootCtx,
		cfg.OTel.ServiceName,
		cfg.OTel.Endpoint,
		cfg.OTel.SamplerRatio,
	)
	if err != nil {
		log.Fatal("tracer init failed", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracer(ctx)
	}()
	log.Info("tracer initialized", zap.String("endpoint", cfg.OTel.Endpoint))

	// ---------- PostgreSQL ----------
	db, err := sqlx.Connect("postgres", cfg.Postgres.DSN())
	if err != nil {
		log.Fatal("postgres connection failed", zap.Error(err))
	}
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(cfg.Postgres.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Postgres.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.Postgres.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.Postgres.ConnMaxIdleTime)
	log.Info("postgres connected")

	// ---------- Migrations ----------
	if err := migrations.Up(rootCtx, db); err != nil {
		log.Fatal("migrations failed", zap.Error(err))
	}
	log.Info("migrations applied")

	// ---------- MinIO ----------
	minioClient, err := minio.NewClient(rootCtx, minio.Options{
		Endpoint:  cfg.MinIO.Endpoint,
		AccessKey: cfg.MinIO.AccessKey,
		SecretKey: cfg.MinIO.SecretKey,
		Bucket:    cfg.MinIO.Bucket,
		UseSSL:    cfg.MinIO.UseSSL,
	}, log)
	if err != nil {
		log.Fatal("minio init failed", zap.Error(err))
	}

	// ---------- Kafka producer ----------
	brokers := strings.Split(cfg.Kafka.Brokers, ",")
	kafkaProducer, err := kafka.NewProducer(brokers, cfg.Kafka.DocumentsTopic, log)
	if err != nil {
		log.Fatal("kafka producer init failed", zap.Error(err))
	}
	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			log.Error("kafka producer close", zap.Error(err))
		}
	}()

	// ---------- Repositories ----------
	docRepo := repository.NewPostgresRepository(db, log)
	outboxRepo := repository.NewOutboxPostgresRepository(db, log)

	// ---------- Services ----------
	creatorService := service.NewCreatorService(docRepo, minioClient, log)

	// ---------- HTTP handlers ----------
	errMapper := handler.NewErrorMapper(log)
	docHandler := handler.New(creatorService, cfg.Document.MaxUploadBytes, errMapper, log)

	// ---------- gRPC server ----------
	grpcSrv := grpcserver.New(":"+cfg.Document.GRPCPort, log)
	docGRPCServer := documentgrpc.NewServer(docRepo)
	grpcSrv.Register(func(s *grpc.Server) {
		documentv1.RegisterDocumentServiceServer(s, docGRPCServer)
		reflection.Register(s) // для grpcurl в dev; отключить в prod
	})
	grpcErr := grpcSrv.Start()

	// ---------- Outbox worker (фоновая горутина) ----------
	outboxWorker := worker.NewOutboxWorker(outboxRepo, kafkaProducer, cfg.Outbox, log)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if err := outboxWorker.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("outbox_worker_failed", zap.Error(err))
		}
	}()

	// ---------- Health checks ----------
	healthHandler := health.NewHandler().
		Register("postgres", func(ctx context.Context) error {
			return db.PingContext(ctx)
		}).
		Register("minio", func(ctx context.Context) error {
			return minioClient.EnsureBucket(ctx)
		})

	// ---------- Gin ----------
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.New()
	router.Use(otelgin.Middleware(
		cfg.OTel.ServiceName,
		otelgin.WithFilter(func(r *http.Request) bool {
			switch r.URL.Path {
			case "/metrics", "/health/live", "/health/ready":
				return false
			}
			return true
		}),
	))
	router.Use(gin.Recovery())
	router.Use(middleware.RequestID())

	// Public
	router.GET("/health/live", healthHandler.Live)
	router.GET("/health/ready", healthHandler.Ready)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API v1
	v1 := router.Group("/api/v1")
	{
		// TODO: подключить auth middleware после интеграции с Gateway
		v1.POST("/documents", docHandler.Create)
	}

	// ---------- HTTP server ----------
	httpSrv := server.New(":"+cfg.Document.HTTPPort, router, log)

	// ---------- pprof server ----------
	pprofSrv := startPprof(cfg.Document.PprofPort, log)

	// ---------- Start ----------
	httpErr := httpSrv.Start()

	// ---------- Wait for shutdown ----------
	select {
	case <-rootCtx.Done():
		log.Info("shutdown signal received")
	case err := <-httpErr:
		if err != nil {
			log.Fatal("http server failed", zap.Error(err))
		}
	case err := <-grpcErr:
		if err != nil {
			log.Fatal("grpc server failed", zap.Error(err))
		}
	}

	// ---------- Graceful shutdown ----------
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx, 10*time.Second); err != nil {
		log.Error("http shutdown error", zap.Error(err))
	}
	grpcSrv.Shutdown(5 * time.Second)
	if pprofSrv != nil {
		if err := pprofSrv.Shutdown(shutdownCtx, 3*time.Second); err != nil {
			log.Error("pprof shutdown error", zap.Error(err))
		}
	}

	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		log.Warn("outbox_worker_shutdown_timeout")
	}

	log.Info("document-service stopped")
}

func startPprof(port string, log *zap.Logger) *server.HTTPServer {
	if port == "" {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := server.New(":"+port, mux, log)
	go func() {
		log.Info("pprof server starting", zap.String("addr", srv.Addr()))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("pprof server failed", zap.Error(err))
		}
	}()
	return srv
}
