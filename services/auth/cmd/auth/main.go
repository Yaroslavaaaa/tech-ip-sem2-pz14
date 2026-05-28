package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"auth-service/internal/grpc"
	"auth-service/internal/handler"
	"auth-service/internal/service"
	authpb "tech-ip-sem2/pkg"
	"tech-ip-sem2/shared/logger"
	"tech-ip-sem2/shared/middleware"

	"go.uber.org/zap"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	httpPort := os.Getenv("AUTH_HTTP_PORT")
	if httpPort == "" {
		httpPort = "8081"
	}

	grpcPort := os.Getenv("AUTH_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "development"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	zapLogger := logger.MustLogger(logger.Config{
		ServiceName: "auth",
		Environment: env,
		LogLevel:    logLevel,
	})
	defer zapLogger.Sync()

	authService := service.NewAuthService()

	authHandler := handler.NewAuthHandler(authService)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/auth/login", authHandler.Login)
	mux.HandleFunc("GET /v1/auth/verify", authHandler.Verify)

	httpHandler := middleware.RequestIDMiddleware(mux)
	httpHandler = middleware.AccessLogMiddleware(zapLogger)(httpHandler)

	httpServer := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      httpHandler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	grpcListener, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		zapLogger.Fatal("Failed to listen gRPC", zap.Error(err))
	}

	grpcServer := gogrpc.NewServer()
	authpb.RegisterAuthServiceServer(grpcServer, grpc.NewAuthServer(authService, zapLogger))
	reflection.Register(grpcServer)

	go func() {
		zapLogger.Info("Auth HTTP service starting",
			zap.String("port", httpPort),
			zap.String("env", env))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zapLogger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	go func() {
		zapLogger.Info("Auth gRPC service starting",
			zap.String("port", grpcPort))
		if err := grpcServer.Serve(grpcListener); err != nil {
			zapLogger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	zapLogger.Info("Shutting down Auth service...")

	grpcServer.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		zapLogger.Fatal("HTTP server forced to shutdown", zap.Error(err))
	}

	zapLogger.Info("Auth service stopped")
}
