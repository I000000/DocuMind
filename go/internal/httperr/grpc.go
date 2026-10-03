package httperr

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FromGRPC преобразует ошибку gRPC в HTTP-статус и публичное сообщение.
// Используется gateway'ем, который вызывает внутренние сервисы по gRPC.
//
// Внутренние детали (stack traces, SQL-ошибки) не попадают в ответ —
// только status.Message(), который сервис контролирует сам.
func FromGRPC(err error) (int, string) {
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, "internal error"
	}

	switch st.Code() {
	case codes.OK:
		return http.StatusOK, ""
	case codes.InvalidArgument:
		return http.StatusBadRequest, st.Message()
	case codes.NotFound:
		return http.StatusNotFound, st.Message()
	case codes.AlreadyExists:
		return http.StatusConflict, st.Message()
	case codes.Unauthenticated:
		return http.StatusUnauthorized, "unauthenticated"
	case codes.PermissionDenied:
		return http.StatusForbidden, "permission denied"
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, "resource exhausted"
	case codes.Unavailable:
		return http.StatusServiceUnavailable, "upstream service unavailable"
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, "upstream timeout"
	default:
		return http.StatusInternalServerError, "internal error"
	}
}
