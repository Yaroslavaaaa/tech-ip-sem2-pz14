package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *zap.Logger
}

type Config struct {
	URL       string
	QueueName string
	Durable   bool
}

// TaskEvent структура события о создании задачи
type TaskEvent struct {
	Event     string    `json:"event"`
	TaskID    string    `json:"task_id"`
	RequestID string    `json:"request_id,omitempty"`
	Timestamp time.Time `json:"ts"`
	Producer  string    `json:"producer"`
	Version   string    `json:"version"`
}

// TaskJob - структура задачи для очереди
type TaskJob struct {
	Job       string `json:"job"`
	TaskID    string `json:"task_id"`
	Attempt   int    `json:"attempt"`
	MessageID string `json:"message_id"`
}

func NewClient(cfg Config, logger *zap.Logger) (*Client, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &Client{
		conn:    conn,
		channel: channel,
		logger:  logger.With(zap.String("component", "rabbitmq")),
	}, nil
}

// ==================== МЕТОДЫ ДЛЯ СОБЫТИЙ (ПЗ13) ====================

// PublishTaskEvent публикует событие о создании задачи
func (c *Client) PublishTaskEvent(ctx context.Context, queueName, taskID, requestID string) error {
	event := TaskEvent{
		Event:     "task.created",
		TaskID:    taskID,
		RequestID: requestID,
		Timestamp: time.Now(),
		Producer:  "tasks-service",
		Version:   "1.0",
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = c.channel.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		c.logger.Error("Failed to publish task event", zap.Error(err))
		return err
	}
	c.logger.Debug("Task event published",
		zap.String("queue", queueName),
		zap.String("task_id", taskID))
	return nil
}

// ==================== МЕТОДЫ ДЛЯ ЗАДАЧ (ПЗ14) ====================

// DeclareQueue объявляет очередь с заданными параметрами
func (c *Client) DeclareQueue(name string, durable bool, deadLetterRoutingKey string) error {
	args := amqp.Table{}
	if deadLetterRoutingKey != "" {
		args["x-dead-letter-exchange"] = ""
		args["x-dead-letter-routing-key"] = deadLetterRoutingKey
	}

	_, err := c.channel.QueueDeclare(
		name,
		durable,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", name, err)
	}
	c.logger.Debug("Queue declared", zap.String("queue", name), zap.Any("args", args))
	return nil
}

// DeclareDLQSetup объявляет основную очередь и DLQ
func (c *Client) DeclareDLQSetup(mainQueue, dlqName string) error {
	// 1. Объявляем DLQ (без специальных аргументов)
	if err := c.DeclareQueue(dlqName, true, ""); err != nil {
		return err
	}

	// 2. Объявляем основную очередь с DLX, указывающим на DLQ
	if err := c.DeclareQueue(mainQueue, true, dlqName); err != nil {
		return err
	}

	c.logger.Info("DLQ setup completed",
		zap.String("main_queue", mainQueue),
		zap.String("dlq", dlqName))
	return nil
}

// PublishJob публикует задачу в очередь
func (c *Client) PublishJob(ctx context.Context, queueName string, job *TaskJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	err = c.channel.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			MessageId:    job.MessageID,
		},
	)
	if err != nil {
		c.logger.Error("Failed to publish job", zap.Error(err))
		return err
	}
	c.logger.Debug("Job published", zap.String("queue", queueName), zap.String("message_id", job.MessageID))
	return nil
}

// Consume подписывается на очередь и возвращает канал с сообщениями
func (c *Client) Consume(queueName string) (<-chan amqp.Delivery, error) {
	deliveries, err := c.channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}
	c.logger.Debug("Consumer registered", zap.String("queue", queueName))
	return deliveries, nil
}

// SetPrefetch устанавливает количество сообщений для prefetch
func (c *Client) SetPrefetch(count int) error {
	err := c.channel.Qos(count, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set prefetch: %w", err)
	}
	c.logger.Debug("Prefetch set", zap.Int("count", count))
	return nil
}

// PublishToDLQ отправляет сообщение в DLQ
func (c *Client) PublishToDLQ(ctx context.Context, dlqName string, originalDelivery amqp.Delivery, reason string) error {
	c.logger.Warn("Publishing to DLQ",
		zap.String("dlq", dlqName),
		zap.String("reason", reason),
		zap.String("message_id", originalDelivery.MessageId))

	err := c.channel.PublishWithContext(ctx,
		"",
		dlqName,
		false,
		false,
		amqp.Publishing{
			ContentType:  originalDelivery.ContentType,
			Body:         originalDelivery.Body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			MessageId:    originalDelivery.MessageId,
			Headers: amqp.Table{
				"x-original-reason": reason,
				"x-original-time":   time.Now().Format(time.RFC3339),
			},
		},
	)
	if err != nil {
		c.logger.Error("Failed to publish to DLQ", zap.Error(err))
		return err
	}
	c.logger.Info("Message sent to DLQ", zap.String("dlq", dlqName))
	return nil
}

// Close закрывает соединения
func (c *Client) Close() error {
	if err := c.channel.Close(); err != nil {
		return err
	}
	return c.conn.Close()
}
