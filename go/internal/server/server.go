package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// HTTPServer — обёртка над http.Server с харденингом и graceful shutdown.
type HTTPServer struct {
	srv    *http.Server
	logger *zap.Logger
}

// New создаёт HTTP-сервер с безопасными таймаутами.
func New(addr string, handler http.Handler, logger *zap.Logger) *HTTPServer {
	return &HTTPServer{
		srv: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,  // защита от Slowloris
			ReadTimeout:       15 * time.Second, // чтение body
			WriteTimeout:      30 * time.Second, // запись ответа
			IdleTimeout:       60 * time.Second, // keep-alive соединения
			MaxHeaderBytes:    1 << 20,          // 1 MiB
		},
		logger: logger,
	}
}

// Addr возвращает адрес, на котором слушает сервер.
func (s *HTTPServer) Addr() string {
	return s.srv.Addr
}

// ListenAndServe блокируется до остановки сервера.
// Возвращает http.ErrServerClosed при корректном shutdown.
func (s *HTTPServer) ListenAndServe() error {
	return s.srv.ListenAndServe()
}

// Start запускает сервер в отдельной горутине и возвращает канал ошибок.
func (s *HTTPServer) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("http server starting", zap.String("addr", s.srv.Addr))
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	return errCh
}

// Shutdown корректно останавливает сервер с заданным таймаутом.
func (s *HTTPServer) Shutdown(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s.logger.Info("http server shutting down")
	return s.srv.Shutdown(ctx)
}
