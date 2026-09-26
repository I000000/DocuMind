package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RateLimit ограничивает количество запросов с одного IP.
// При недоступности Redis — пропускает запросы (fail-open).
func RateLimit(rdb *redis.Client, limit int, window time.Duration, logger *zap.Logger) gin.HandlerFunc {
	limiter := redis_rate.NewLimiter(rdb)

	return func(c *gin.Context) {
		key := "rl:" + c.ClientIP()

		res, err := limiter.Allow(c.Request.Context(), key, redis_rate.Limit{
			Rate:   limit,
			Burst:  limit,
			Period: window,
		})
		if err != nil {
			logger.Warn("rate limiter error, allowing request",
				zap.Error(err),
				zap.String("ip", c.ClientIP()),
			)
			c.Next()
			return
		}

		c.Header("X-RateLimit-Limit", strconv.Itoa(res.Limit.Rate))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))

		if res.Allowed == 0 {
			c.Header("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())+1))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			return
		}

		c.Next()
	}
}
