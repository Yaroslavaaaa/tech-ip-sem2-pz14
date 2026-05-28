package grpc

import (
	"context"

	"auth-service/internal/service"
	authpb "tech-ip-sem2/pkg"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthServer struct {
	authpb.UnimplementedAuthServiceServer
	authService *service.AuthService
	logger      *zap.Logger
}

func NewAuthServer(authService *service.AuthService, logger *zap.Logger) *AuthServer {
	return &AuthServer{
		authService: authService,
		logger:      logger.With(zap.String("component", "grpc_server")),
	}
}

func (s *AuthServer) Verify(ctx context.Context, req *authpb.VerifyRequest) (*authpb.VerifyResponse, error) {
	requestID := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("x-request-id"); len(values) > 0 {
			requestID = values[0]
		}
	}

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("grpc_method", "Verify"),
	)

	token := req.GetToken()
	log.Debug("Verify called",
		zap.String("token_prefix", token[:min(5, len(token))]),
		zap.Bool("has_token", token != ""))

	if token == "" {
		log.Warn("Empty token received")
		return nil, status.Error(codes.Unauthenticated, "token is empty")
	}

	username, valid := s.authService.Verify(token)

	if !valid {
		log.Info("Invalid token",
			zap.String("token_prefix", token[:min(5, len(token))]))
		return &authpb.VerifyResponse{
			Valid: false,
			Error: "invalid token",
		}, nil
	}

	log.Info("Token verified successfully",
		zap.String("subject", username))

	return &authpb.VerifyResponse{
		Valid:   true,
		Subject: username,
	}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
