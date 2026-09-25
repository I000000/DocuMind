package health

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Checker — функция проверки зависимости.
type Checker func(ctx context.Context) error

type Handler struct {
	checks map[string]Checker
}

func NewHandler() *Handler {
	return &Handler{checks: make(map[string]Checker)}
}

func (h *Handler) Register(name string, c Checker) *Handler {
	h.checks[name] = c
	return h
}

// Live — процесс жив. Ничего не проверяет.
func (h *Handler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Ready — все зависимости доступны.
func (h *Handler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(h.checks))

	var wg sync.WaitGroup
	for name, check := range h.checks {
		wg.Add(1)
		go func(name string, check Checker) {
			defer wg.Done()
			results <- result{name: name, err: check(ctx)}
		}(name, check)
	}
	wg.Wait()
	close(results)

	failures := make(map[string]string)
	for r := range results {
		if r.err != nil {
			failures[r.name] = r.err.Error()
		}
	}

	if len(failures) > 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":   "unavailable",
			"failures": failures,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
