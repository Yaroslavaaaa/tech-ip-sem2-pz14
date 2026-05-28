package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisClient struct {
	client  *redis.Client
	logger  *zap.Logger
	ttl     time.Duration
	jitter  time.Duration
	enabled bool
}

type Config struct {
	Addr     string
	Password string
	DB       int
	TTL      int
	Jitter   int
}

func NewRedisClient(cfg Config, logger *zap.Logger) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolTimeout:  1 * time.Second,
		MaxRetries:   0,
		PoolSize:     1,
	})

	// Проверяем подключение с коротким таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	enabled := true
	if err := client.Ping(ctx).Err(); err != nil {
		logger.Warn("Redis connection failed, cache disabled", zap.Error(err))
		enabled = false
	} else {
		logger.Info("Redis connected successfully")
	}

	return &RedisClient{
		client:  client,
		logger:  logger.With(zap.String("component", "redis_cache")),
		ttl:     time.Duration(cfg.TTL) * time.Second,
		jitter:  time.Duration(cfg.Jitter) * time.Second,
		enabled: enabled,
	}, nil
}

// IsEnabled возвращает, доступен ли Redis
func (c *RedisClient) IsEnabled() bool {
	return c.enabled
}

// GetTTL возвращает TTL с добавленным jitter
func (c *RedisClient) GetTTL() time.Duration {
	if !c.enabled {
		return 0
	}
	if c.jitter == 0 {
		return c.ttl
	}
	jitterMs := time.Duration(rand.Int63n(int64(c.jitter)))
	return c.ttl + jitterMs
}

// Get возвращает значение из кэша по ключу
func (c *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	if !c.enabled {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		c.logger.Debug("Redis get failed (non-critical)", zap.String("key", key), zap.Error(err))
		return nil, nil
	}
	c.logger.Debug("Redis cache hit", zap.String("key", key))
	return val, nil
}

// Set сохраняет значение в кэш
func (c *RedisClient) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if !c.enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	err = c.client.Set(ctx, key, data, ttl).Err()
	if err != nil {
		c.logger.Debug("Redis set failed (non-critical)", zap.String("key", key), zap.Error(err))
		return nil
	}
	return nil
}

// Delete удаляет ключ из кэша
func (c *RedisClient) Delete(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := c.client.Del(ctx, key).Err()
	if err != nil {
		c.logger.Debug("Redis delete failed (non-critical)", zap.String("key", key), zap.Error(err))
		return nil
	}
	return nil
}

// Close закрывает соединение с Redis
func (c *RedisClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
