package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// InitTracer настраивает глобальный TracerProvider и propagator.
// Возвращает shutdown-функцию — вызывать при завершении приложения.
//
// endpoint — OTLP HTTP endpoint Jaeger, например "http://localhost:4318".
// samplerRatio — доля трейсов для записи.
func InitTracer(ctx context.Context, serviceName, endpoint string, samplerRatio float64) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(stripScheme(endpoint)),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(
			sdktrace.TraceIDRatioBased(samplerRatio),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// ExtractFromCarrier восстанавливает trace context из W3C traceparent.
// Используется outbox-worker'ом: событие хранит traceparent в БД,
// worker извлекает его и продолжает span в Kafka producer'е.
func ExtractFromCarrier(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// InjectToCarrier извлекает trace context из ctx и возвращает как map.
// Используется при создании outbox-события: traceparent сохраняется в БД,
// чтобы outbox-worker мог продолжить span при публикации в Kafka.
func InjectToCarrier(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	out := make(map[string]string, len(carrier))
	for k, v := range carrier {
		out[k] = v
	}
	return out
}

// stripScheme убирает "http://" или "https://" из URL — otlptracehttp
// ожидает host:port без схемы.
func stripScheme(endpoint string) string {
	for _, prefix := range []string{"http://", "https://"} {
		if len(endpoint) > len(prefix) && endpoint[:len(prefix)] == prefix {
			return endpoint[len(prefix):]
		}
	}
	return endpoint
}
