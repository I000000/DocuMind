package grpcserver

import (
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server — обёртка над grpc.Server с логированием и graceful shutdown.
type Server struct {
	grpc   *grpc.Server
	addr   string
	logger *zap.Logger
}

// New создаёт gRPC-сервер с дефолтной цепочкой интерсепторов:
// recovery → logging → tracing → metrics → [extra interceptors].
func New(
	addr string,
	logger *zap.Logger,
	extraInterceptors ...grpc.UnaryServerInterceptor,
) *Server {
	chain := []grpc.UnaryServerInterceptor{
		RecoveryInterceptor(logger),
		LoggingInterceptor(logger),
		TracingInterceptor(),
		MetricsInterceptor(),
	}
	chain = append(chain, extraInterceptors...)

	return &Server{
		grpc:   grpc.NewServer(grpc.ChainUnaryInterceptor(chain...)),
		addr:   addr,
		logger: logger,
	}
}

// Register позволяет зарегистрировать сервисы после создания.
func (s *Server) Register(fn func(*grpc.Server)) {
	fn(s.grpc)
}

// Start запускает сервер в горутине. Возвращает канал ошибок.
func (s *Server) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		lis, err := net.Listen("tcp", s.addr)
		if err != nil {
			errCh <- fmt.Errorf("listen: %w", err)
			return
		}

		s.logger.Info("grpc server starting", zap.String("addr", s.addr))
		if err := s.grpc.Serve(lis); err != nil {
			errCh <- fmt.Errorf("serve: %w", err)
			return
		}
		errCh <- nil
	}()
	return errCh
}

// Shutdown корректно останавливает сервер с таймаутом.
func (s *Server) Shutdown(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("grpc server stopped gracefully")
	case <-time.After(timeout):
		s.logger.Warn("grpc graceful stop timed out, forcing")
		s.grpc.Stop()
	}
}
