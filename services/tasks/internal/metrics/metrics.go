package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "route", "status"},
	)

	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 1, 3, 10},
		},
		[]string{"method", "route"},
	)

	InFlightRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_in_flight_requests",
			Help: "Current number of in-flight HTTP requests",
		},
	)
)

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		InFlightRequests.Inc()
		defer InFlightRequests.Dec()

		route := getRoutePattern(r.URL.Path)

		start := time.Now()

		wrapped := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start).Seconds()
		RequestDuration.WithLabelValues(r.Method, route).Observe(duration)

		status := strconv.Itoa(wrapped.statusCode)
		RequestsTotal.WithLabelValues(r.Method, route, status).Inc()
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func getRoutePattern(path string) string {
	switch {
	case len(path) > 10 && path[:10] == "/v1/tasks/":
		return "/v1/tasks/{id}"
	case path == "/v1/tasks":
		return "/v1/tasks"
	case path == "/metrics":
		return "/metrics"
	default:
		return path
	}
}
