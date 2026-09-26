package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/I000000/DocuMind/internal/auth"
)

// RequireAnyRole требует наличия хотя бы одной из указанных ролей.
func RequireAnyRole(roles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		user, ok := auth.UserFromContext(c.Request.Context())
		if !ok || user == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "unauthenticated",
			})
			return
		}

		if !user.HasAnyRole(roles...) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error":             "insufficient permissions",
				"required_any_role": roles,
			})
			return
		}

		c.Next()
	}
}
