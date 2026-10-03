package grpcserver

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/I000000/DocuMind/internal/auth"
)

// AuthInterceptor проверяет Bearer-токен из gRPC metadata и кладёт
// аутентифицированного пользователя в контекст.
//
// Токен ожидается в metadata "authorization" в формате "Bearer <token>".
// Gateway прокидывает его из входящего HTTP-запроса.
//
// Публичные методы (health, reflection) пропускаются без проверки.
type AuthInterceptor struct {
	provider      *auth.Provider
	publicMethods map[string]bool
}

// NewAuthInterceptor создаёт интерсептор аутентификации.
// publicMethods — полные имена методов, которые не требуют токена
func NewAuthInterceptor(provider *auth.Provider, publicMethods ...string) *AuthInterceptor {
	public := make(map[string]bool, len(publicMethods))
	for _, m := range publicMethods {
		public[m] = true
	}
	return &AuthInterceptor{
		provider:      provider,
		publicMethods: public,
	}
}

func (i *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if i.publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		values := md.Get("authorization")
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}

		raw := values[0]
		parts := strings.SplitN(raw, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization format")
		}

		idToken, err := i.provider.Verify(ctx, strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
		}

		var rawClaims map[string]interface{}
		if err := idToken.Claims(&rawClaims); err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid token claims")
		}

		user := auth.ExtractUser(idToken.Subject, rawClaims)
		ctx = auth.WithUser(ctx, user)

		return handler(ctx, req)
	}
}
