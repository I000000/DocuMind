package grpcserver

import (
	"context"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryInterceptor ловит панику в хендлере и возвращает Internal вместо
// падения всего процесса.
func RecoveryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("grpc_panic",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
					zap.ByteString("stack", debug.Stack()),
				)
				err = status.Error(grpccodes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// LoggingInterceptor логирует каждый вызов: метод, длительность, код.
func LoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		logger.Info("grpc_request",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", time.Since(start)),
			zap.String("code", status.Code(err).String()),
		)
		return resp, err
	}
}

// TracingInterceptor создаёт span на каждый gRPC-вызов и продолжает trace
// из входящих метаданных (клиент прокидывает traceparent через headers).
func TracingInterceptor() grpc.UnaryServerInterceptor {
	tracer := otel.Tracer("grpc.server")
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		ctx, span := tracer.Start(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		resp, err := handler(ctx, req)

		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		}
		return resp, err
	}
}

// MetricsInterceptor считает количество и длительность gRPC-запросов.
func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		code := status.Code(err).String()
		grpcRequestsTotal.WithLabelValues(info.FullMethod, code).Inc()
		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())

		return resp, err
	}
}
