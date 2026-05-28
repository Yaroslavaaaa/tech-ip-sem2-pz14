package client

import (
	"context"
	"fmt"
	"time"

	authpb "tech-ip-sem2/pkg"
	"tech-ip-sem2/shared/logger"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthClient struct {
	client  authpb.AuthServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
	logger  *zap.Logger
}

func NewAuthClient(addr string, timeout time.Duration, parentLogger *zap.Logger) (*AuthClient, error) {
	logger := parentLogger.With(zap.String("component", "auth_client"))

	logger.Info("Connecting to auth gRPC server", zap.String("addr", addr))

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		logger.Error("Failed to connect to auth server", zap.Error(err))
		return nil, fmt.Errorf("failed to connect to auth gRPC server: %w", err)
	}

	logger.Info("Connected to auth server successfully")

	client := authpb.NewAuthServiceClient(conn)

	return &AuthClient{
		client:  client,
		conn:    conn,
		timeout: timeout,
		logger:  logger,
	}, nil
}

func (c *AuthClient) Close() error {
	c.logger.Debug("Closing auth client connection")
	return c.conn.Close()
}

func (c *AuthClient) VerifyToken(ctx context.Context, token string) (string, error) {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := c.logger.With(
		zap.String("request_id", requestID),
		zap.String("grpc_method", "Verify"),
	)

	log.Debug("Calling Verify via gRPC", zap.String("token_prefix", token[:min(5, len(token))]))

	md := metadata.New(map[string]string{
		"x-request-id": requestID,
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := c.client.Verify(ctx, &authpb.VerifyRequest{
		Token: token,
	})

	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.DeadlineExceeded:
				log.Error("Auth service deadline exceeded",
					zap.Duration("timeout", c.timeout))
				return "", fmt.Errorf("auth service timeout")
			case codes.Unavailable:
				log.Error("Auth service unavailable")
				return "", fmt.Errorf("auth service unavailable")
			case codes.Unauthenticated:
				log.Warn("Token invalid",
					zap.String("grpc_error", st.Message()))
				return "", fmt.Errorf("token invalid: %s", st.Message())
			default:
				log.Error("Auth service error",
					zap.String("grpc_code", st.Code().String()),
					zap.String("grpc_error", st.Message()))
				return "", fmt.Errorf("auth service error: %v", st.Message())
			}
		}
		log.Error("Failed to verify token", zap.Error(err))
		return "", fmt.Errorf("failed to verify token: %w", err)
	}

	if !resp.GetValid() {
		log.Warn("Token invalid",
			zap.String("error", resp.GetError()))
		return "", fmt.Errorf("token invalid: %s", resp.GetError())
	}

	log.Info("Token verified successfully",
		zap.String("subject", resp.GetSubject()))

	return resp.GetSubject(), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
