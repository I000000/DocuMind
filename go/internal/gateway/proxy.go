package gateway

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"go.uber.org/zap"
)

// NewUploadProxy создаёт reverse proxy для upload-запросов
// к document-service.
//
// Особенности:
//   - Тело запроса стримится, не буферизируется.
//   - Authorization header пробрасывается как есть — document-service
//     сам проверит токен.
//   - X-Request-ID и traceparent пробрасываются автоматически через
//     ProxyRequest (Rewrite копирует все headers из In в Out).
func NewUploadProxy(targetURL string, logger *zap.Logger) (http.Handler, error) {
	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	proxy := &httputil.ReverseProxy{
		// Rewrite вызывается для подготовки исходящего запроса.
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			// SetXForwarded проставляет X-Forwarded-* из входящего запроса,
			// игнорируя то, что клиент мог прислать сам (защита от IP-спуфинга).
			pr.SetXForwarded()
			// Сохраняем оригинальный Host клиента.
			pr.Out.Host = pr.In.Host
		},

		ModifyResponse: func(resp *http.Response) error {
			logger.Debug("upload_proxy_response", zap.Int("status", resp.StatusCode))
			return nil
		},

		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Error("upload_proxy_failed",
				zap.String("target", targetURL),
				zap.Error(err),
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"upstream service unavailable"}`))
		},
	}

	return proxy, nil
}
