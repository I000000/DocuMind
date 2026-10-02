package grpcserver

import "github.com/prometheus/client_golang/prometheus"

var (
	grpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_server_handled_total",
			Help: "Total number of gRPC requests handled, by method and code.",
		},
		[]string{"method", "code"},
	)

	grpcRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "Duration of gRPC requests in seconds, by method.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(grpcRequestsTotal, grpcRequestDuration)
}
