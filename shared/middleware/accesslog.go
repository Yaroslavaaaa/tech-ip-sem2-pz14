package middleware

import (
	"net/http"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ResponseWriterWrapper struct {
	http.ResponseWriter
	StatusCode int
	bytes      int
}

func (rw *ResponseWriterWrapper) WriteHeader(code int) {
	rw.StatusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *ResponseWriterWrapper) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += n
	return n, err
}

func AccessLogMiddleware(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			wrapped := &ResponseWriterWrapper{
				ResponseWriter: w,
				StatusCode:     http.StatusOK,
			}

			next.ServeHTTP(wrapped, r)

			requestID := wrapped.Header().Get("X-Request-ID")
			if requestID == "" {
				requestID = "unknown"
			}

			duration := time.Since(start)
			durationMs := duration.Milliseconds()

			var level zapcore.Level
			switch {
			case wrapped.StatusCode >= 500:
				level = zapcore.ErrorLevel
			case wrapped.StatusCode >= 400:
				level = zapcore.WarnLevel
			default:
				level = zapcore.InfoLevel
			}

			log.Check(level, "request completed").Write(
				zap.String("request_id", requestID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", wrapped.StatusCode),
				zap.Int64("duration_ms", durationMs),
				zap.Int("bytes", wrapped.bytes),
				zap.String("remote_addr", r.RemoteAddr),
				zap.String("user_agent", r.UserAgent()),
			)
		})
	}
}
