package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/I000000/DocuMind/internal/config"
	"github.com/I000000/DocuMind/internal/health"
	"github.com/I000000/DocuMind/internal/logger"
	"github.com/I000000/DocuMind/internal/server"
)

func main() {
	// ---------- Config ----------
	cfg, err := config.Load()
	if err != nil {
		// Логгера ещё нет, падаем на стандартный
		panic("config error: " + err.Error())
	}

	// ---------- Logger ----------
	log, err := logger.New(cfg.App.Env, cfg.App.LogLevel)
	if err != nil {
		panic("logger error: " + err.Error())
	}
	defer func() { _ = log.Sync() }()

	log.Info("starting documind gateway",
		zap.String("env", cfg.App.Env),
		zap.String("version", "dev"),
	)

	// ---------- Context с отменой по сигналу ----------
	rootCtx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- Redis ----------
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(rootCtx).Err(); err != nil {
		log.Warn("redis ping failed, continuing without cache", zap.Error(err))
	}

	// ---------- Health checks ----------
	healthHandler := health.NewHandler().
		Register("redis", func(ctx context.Context) error {
			return rdb.Ping(ctx).Err()
		})

	// TODO: добавить postgres, grpc-клиенты, kafka producer

	// ---------- Gin ----------
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.New()
	router.Use(gin.Recovery())

	// TODO: request ID, otelgin, metrics, rate limit, auth, RBAC

	router.GET("/health/live", healthHandler.Live)
	router.GET("/health/ready", healthHandler.Ready)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Заглушка
	router.GET("/api/v1/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"pong": true})
	})

	// ---------- HTTP Server ----------
	httpSrv := server.New(":"+cfg.App.ServerPort, router, log)

	// ---------- pprof Server (отдельный порт) ----------
	pprofSrv := startPprof(cfg.App.PprofPort, log)

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
	}

	// ---------- Graceful shutdown ----------
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx, 10*time.Second); err != nil {
		log.Error("http shutdown error", zap.Error(err))
	}
	if pprofSrv != nil {
		if err := pprofSrv.Shutdown(shutdownCtx, 3*time.Second); err != nil {
			log.Error("pprof shutdown error", zap.Error(err))
		}
	}

	log.Info("gateway stopped")
}

// startPprof поднимает pprof на отдельном порту
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
