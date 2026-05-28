package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	logger    *log.Logger
	queueName string
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

func NewClient(cfg Config, logger *log.Logger) (*Client, error) {
	if logger == nil {
		logger = log.New(os.Stdout, "[RABBITMQ] ", log.LstdFlags)
	}

	logger.Printf("Connecting to RabbitMQ at %s", cfg.URL)

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Объявляем очередь (durable=true — переживает рестарт)
	_, err = channel.QueueDeclare(
		cfg.QueueName, // name
		cfg.Durable,   // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	logger.Printf("RabbitMQ connected successfully. Queue: %s", cfg.QueueName)

	return &Client{
		conn:      conn,
		channel:   channel,
		logger:    logger,
		queueName: cfg.QueueName,
	}, nil
}

// SetPrefetch устанавливает количество сообщений, которые consumer может получить одновременно
func (c *Client) SetPrefetch(count int) error {
	err := c.channel.Qos(
		count, // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set prefetch: %w", err)
	}
	c.logger.Printf("Prefetch set to %d", count)
	return nil
}

// Consume подписывается на очередь и возвращает канал с сообщениями
func (c *Client) Consume(queueName string) (<-chan amqp.Delivery, error) {
	deliveries, err := c.channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack (false — ручной ack)
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}
	c.logger.Printf("Started consuming from queue: %s", queueName)
	return deliveries, nil
}

// Publish публикует сообщение в очередь
func (c *Client) Publish(ctx context.Context, queueName string, body []byte) error {
	err := c.channel.PublishWithContext(ctx,
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent, // persistent message
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		c.logger.Printf("Failed to publish message: %v", err)
		return err
	}
	c.logger.Printf("Message published to queue: %s", queueName)
	return nil
}

// PublishTaskEvent публикует событие о создании задачи
func (c *Client) PublishTaskEvent(ctx context.Context, taskID, requestID string) error {
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

	return c.Publish(ctx, c.queueName, body)
}

// Close закрывает соединения
func (c *Client) Close() error {
	if err := c.channel.Close(); err != nil {
		return err
	}
	return c.conn.Close()
}
