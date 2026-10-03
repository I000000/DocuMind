package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GinKey — ключ для gin.Context.Set.
const GinKey = "auth.user"

// Middleware — аутентификация запросов через OIDC.
type Middleware struct {
	provider *Provider
	logger   *zap.Logger
}

func NewMiddleware(provider *Provider, logger *zap.Logger) *Middleware {
	return &Middleware{provider: provider, logger: logger}
}

// Authenticate проверяет Bearer-токен и кладёт User в контекст.
// При ошибке — 401 и прерывает цепочку.
func (m *Middleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			abortUnauthorized(c, "missing Authorization header")
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			abortUnauthorized(c, "invalid Authorization header format, expected 'Bearer <token>'")
			return
		}

		rawToken := strings.TrimSpace(parts[1])

		idToken, err := m.provider.Verify(c.Request.Context(), rawToken)
		if err != nil {
			m.logger.Warn("token verification failed", zap.Error(err))
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		var rawClaims map[string]interface{}
		if err := idToken.Claims(&rawClaims); err != nil {
			m.logger.Warn("failed to parse claims", zap.Error(err))
			abortUnauthorized(c, "invalid token claims")
			return
		}

		user := ExtractUser(idToken.Subject, rawClaims)

		ctx := WithUser(c.Request.Context(), user)
		c.Request = c.Request.WithContext(ctx)
		c.Set(GinKey, user)

		c.Next()
	}
}

// extractUser парсит Keycloak-специфичные claims в User.
func ExtractUser(subject string, claims map[string]interface{}) *User {
	u := &User{Subject: subject}

	if v, ok := claims["email"].(string); ok {
		u.Email = v
	}
	if v, ok := claims["preferred_username"].(string); ok {
		u.PreferredUsername = v
	}

	// Keycloak хранит роли в realm_access.roles
	if ra, ok := claims["realm_access"].(map[string]interface{}); ok {
		if roles, ok := ra["roles"].([]interface{}); ok {
			for _, r := range roles {
				if s, ok := r.(string); ok {
					u.Roles = append(u.Roles, s)
				}
			}
		}
	}

	return u
}

func abortUnauthorized(c *gin.Context, msg string) {
	c.Header("WWW-Authenticate", `Bearer realm="documind"`)
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": msg})
}
