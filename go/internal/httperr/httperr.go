package httperr

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Rule связывает sentinel-ошибку с HTTP-статусом и публичным сообщением.
// Публичное сообщение попадает в JSON — не включай туда internal-детали.
type Rule struct {
	Err     error
	Status  int
	Message string
}

// Mapper применяет правила в порядке добавления. Первое совпадение выигрывает.
type Mapper struct {
	rules  []Rule
	logger *zap.Logger
}

func New(logger *zap.Logger, rules ...Rule) *Mapper {
	return &Mapper{rules: rules, logger: logger}
}

// Respond логирует ошибку и пишет JSON-ответ с маппленным статусом.
//
// Неиспользуемые (unmapped) ошибки → 500 с общим сообщением. Оригинал
// не утекает клиенту — только в логи.
func (m *Mapper) Respond(c *gin.Context, err error) {
	for _, rule := range m.rules {
		if errors.Is(err, rule.Err) {
			m.logger.Warn("handler_error",
				zap.Error(err),
				zap.Int("status", rule.Status),
			)
			c.JSON(rule.Status, gin.H{"error": rule.Message})
			return
		}
	}

	m.logger.Error("unhandled_handler_error", zap.Error(err))
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
}
