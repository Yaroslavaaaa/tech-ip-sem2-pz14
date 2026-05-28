package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"tasks-service/internal/cache"
	"tasks-service/internal/client"
	"tasks-service/internal/csrf"
	"tasks-service/internal/handler"
	"tasks-service/internal/jobs"
	"tasks-service/internal/metrics"
	"tasks-service/internal/repository"
	"tasks-service/internal/service"
	"tech-ip-sem2/shared/logger"
	"tech-ip-sem2/shared/middleware"
	"tech-ip-sem2/shared/rabbitmq"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	port := os.Getenv("TASKS_PORT")
	if port == "" {
		port = "8082"
	}

	authGRPCAddr := os.Getenv("AUTH_GRPC_ADDR")
	if authGRPCAddr == "" {
		authGRPCAddr = "localhost:50051"
	}

	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://tasks_user:tasks_pass@localhost:5433/tasks_db?sslmode=disable"
	}

	// ----- Настройки Redis -----
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		if db, err := strconv.Atoi(dbStr); err == nil {
			redisDB = db
		}
	}
	cacheTTL := 120 // секунд
	if ttlStr := os.Getenv("CACHE_TTL_SECONDS"); ttlStr != "" {
		if ttl, err := strconv.Atoi(ttlStr); err == nil {
			cacheTTL = ttl
		}
	}
	cacheJitter := 30 // секунд
	if jitterStr := os.Getenv("CACHE_TTL_JITTER_SECONDS"); jitterStr != "" {
		if jitter, err := strconv.Atoi(jitterStr); err == nil {
			cacheJitter = jitter
		}
	}
	// --------------------------

	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "development"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "tasks-default"
	}

	zapLogger := logger.MustLogger(logger.Config{
		ServiceName: "tasks",
		Environment: env,
		LogLevel:    logLevel,
	})
	defer zapLogger.Sync()

	zapLogger.Info("Configuration",
		zap.String("db_url", dbURL),
		zap.String("redis_addr", redisAddr),
		zap.Int("cache_ttl", cacheTTL),
		zap.Int("cache_jitter", cacheJitter),
	)

	// --- Auth client ---
	authClient, err := client.NewAuthClient(authGRPCAddr, 2*time.Second, zapLogger)
	if err != nil {
		zapLogger.Fatal("Failed to create auth client", zap.Error(err))
	}
	defer authClient.Close()

	// --- DB ---
	db, err := repository.NewPostgres(dbURL)
	if err != nil {
		zapLogger.Fatal("Failed to connect DB", zap.Error(err))
	}

	repo, err := repository.NewTaskRepository(db)
	if err != nil {
		zapLogger.Fatal("Failed to init repository", zap.Error(err))
	}

	// --- Redis Cache (опционально, при ошибке продолжаем без кэша) ---
	// --- Redis Cache ---
	redisCache, err := cache.NewRedisClient(cache.Config{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
		TTL:      cacheTTL,
		Jitter:   cacheJitter,
	}, zapLogger)
	if err != nil {
		zapLogger.Warn("Redis init failed, continuing without cache", zap.Error(err))
		redisCache = nil // продолжаем без кэша
	}
	if redisCache != nil {
		defer redisCache.Close()
	}

	// --- RabbitMQ ---
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}
	rabbitQueue := os.Getenv("RABBIT_QUEUE_NAME")
	if rabbitQueue == "" {
		rabbitQueue = "task_events"
	}

	var rabbitClient *rabbitmq.Client
	if rabbitURL != "" {
		rabbitClient, err = rabbitmq.NewClient(rabbitmq.Config{
			URL:       rabbitURL,
			QueueName: rabbitQueue,
			Durable:   true,
		}, zapLogger)
		if err != nil {
			zapLogger.Warn("RabbitMQ connection failed, continuing without", zap.Error(err))
			rabbitClient = nil // Best effort — продолжаем работу
		} else {
			defer rabbitClient.Close()
			zapLogger.Info("RabbitMQ connected", zap.String("url", rabbitURL))
		}
	}

	// Создаем JobPublisher
	var jobPublisher *jobs.JobPublisher
	if rabbitClient != nil {
		jobQueueName := os.Getenv("RABBIT_JOB_QUEUE")
		if jobQueueName == "" {
			jobQueueName = "task_jobs"
		}
		jobPublisher = jobs.NewJobPublisher(rabbitClient, jobQueueName, zapLogger)
	}

	// --- Service ---
	taskService := service.NewTaskService(authClient, repo, redisCache, rabbitClient, zapLogger)
	taskHandler := handler.NewTaskHandler(taskService, zapLogger, instanceID, jobPublisher)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/tasks", taskHandler.CreateTask)
	mux.HandleFunc("GET /v1/tasks", taskHandler.GetTasks)
	mux.HandleFunc("GET /v1/tasks/{id}", taskHandler.GetTask)
	mux.HandleFunc("PATCH /v1/tasks/{id}", taskHandler.UpdateTask)
	mux.HandleFunc("DELETE /v1/tasks/{id}", taskHandler.DeleteTask)
	mux.HandleFunc("GET /v1/tasks/search", taskHandler.SearchTasks)
	mux.HandleFunc("POST /v1/jobs/process-task", taskHandler.CreateJob)
	zapLogger.Info("Registered route", zap.String("method", "POST"), zap.String("path", "/v1/jobs/process-task"))

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Instance-ID", instanceID) // Добавляем для диагностики
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":    "ok",
			"instance":  instanceID,
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test ok"))
	})
	zapLogger.Info("✅ TEST ROUTE REGISTERED", zap.String("path", "/test"))

	handler := metrics.MetricsMiddleware(mux)
	handler = csrf.CSRFMiddleware(handler)
	handler = middleware.RequestIDMiddleware(handler)
	handler = securityHeadersMiddleware(handler)
	handler = middleware.AccessLogMiddleware(zapLogger)(handler)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	go func() {
		zapLogger.Info("Tasks service starting",
			zap.String("port", port),
			zap.String("env", env),
			zap.String("auth_addr", authGRPCAddr),
			zap.String("db_url", dbURL),
			zap.String("redis_addr", redisAddr),
			zap.Bool("cache_enabled", redisCache != nil),
		)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zapLogger.Fatal("Server failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	zapLogger.Info("Shutting down Tasks service...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		zapLogger.Fatal("Server forced to shutdown", zap.Error(err))
	}

	zapLogger.Info("Tasks service stopped")
}

func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}
