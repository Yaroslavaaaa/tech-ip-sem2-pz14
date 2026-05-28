package consumer

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"worker-service/internal/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	RabbitURL string
	QueueName string
	Prefetch  int
}

type Worker struct {
	config Config
	client *rabbitmq.Client
	logger *log.Logger
}

func NewWorker(cfg Config) (*Worker, error) {
	// Создаем логгер для worker
	logger := log.New(os.Stdout, "[WORKER] ", log.LstdFlags)

	client, err := rabbitmq.NewClient(rabbitmq.Config{
		URL:       cfg.RabbitURL,
		QueueName: cfg.QueueName,
		Durable:   true,
	}, logger)
	if err != nil {
		return nil, err
	}

	// Устанавливаем prefetch для контроля нагрузки
	if err := client.SetPrefetch(cfg.Prefetch); err != nil {
		client.Close()
		return nil, err
	}

	return &Worker{
		config: cfg,
		client: client,
		logger: logger,
	}, nil
}

func (w *Worker) Start() error {
	// Подписываемся на очередь
	deliveries, err := w.client.Consume(w.config.QueueName)
	if err != nil {
		return err
	}

	w.logger.Printf("Worker started. Waiting for messages on queue: %s", w.config.QueueName)

	// Обработка сообщений в горутине
	go func() {
		for msg := range deliveries {
			w.handleMessage(msg)
		}
	}()

	// Ожидаем сигнал завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	w.logger.Println("Shutting down worker...")
	return w.client.Close()
}

func (w *Worker) handleMessage(msg amqp.Delivery) {
	w.logger.Printf("Received message: %s", msg.Body)

	var event rabbitmq.TaskEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		w.logger.Printf("Failed to parse message: %v", err)
		msg.Nack(false, true)
		return
	}

	w.logger.Printf("[EVENT] %s for task %s", event.Event, event.TaskID)
	if event.RequestID != "" {
		w.logger.Printf("  Request-ID: %s", event.RequestID)
	}
	w.logger.Printf("  Producer: %s", event.Producer)

	// Успешная обработка — подтверждаем
	msg.Ack(false)
}
