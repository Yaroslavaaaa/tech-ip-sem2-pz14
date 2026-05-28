package middleware

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsMiddleware снимает три группы метрик с каждого HTTP-запроса.
// Метрики передаются снаружи, чтобы middleware не зависел от internal-пакета
// конкретного сервиса.
func MetricsMiddleware(
	requestsTotal *prometheus.CounterVec,
	requestDuration *prometheus.HistogramVec,
	inFlight prometheus.Gauge,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := normalizeRoute(r)

			inFlight.Inc()
			defer inFlight.Dec()

			start := time.Now()

			wrapped := &ResponseWriterWrapper{
				ResponseWriter: w,
				StatusCode:     http.StatusOK,
			}

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start).Seconds()
			status := fmt.Sprintf("%d", wrapped.StatusCode)

			requestsTotal.WithLabelValues(r.Method, route, status).Inc()
			requestDuration.WithLabelValues(r.Method, route).Observe(duration)
		})
	}
}

func normalizeRoute(r *http.Request) string {
	path := r.URL.Path
	switch {
	case path == "/v1/tasks" || path == "/v1/tasks/":
		return "/v1/tasks"
	case len(path) > 10 && path[:10] == "/v1/tasks/":
		return "/v1/tasks/{id}"
	case path == "/metrics":
		return "/metrics"
	default:
		return path
	}
}
