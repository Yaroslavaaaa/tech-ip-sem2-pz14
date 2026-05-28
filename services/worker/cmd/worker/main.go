package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"worker-service/internal/processor"
	"worker-service/internal/store"

	"go.uber.org/zap"

	"tech-ip-sem2/shared/rabbitmq"
)

func main() {
	// Настройка логгера (структурированное логирование)
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Получаем конфигурацию из переменных окружения
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	// Основная очередь задач
	mainQueue := os.Getenv("MAIN_QUEUE")
	if mainQueue == "" {
		mainQueue = "task_jobs"
	}

	// DLQ (Dead Letter Queue)
	dlqName := os.Getenv("DLQ_NAME")
	if dlqName == "" {
		dlqName = "task_jobs_dlq"
	}

	// Максимальное количество попыток
	maxAttempts := 3

	// Prefetch (сколько сообщений обрабатывать параллельно)
	prefetch := 1

	logger.Info("Worker configuration",
		zap.String("rabbit_url", rabbitURL),
		zap.String("main_queue", mainQueue),
		zap.String("dlq_name", dlqName),
		zap.Int("max_attempts", maxAttempts),
		zap.Int("prefetch", prefetch))

	// Подключение к RabbitMQ
	client, err := rabbitmq.NewClient(rabbitmq.Config{
		URL:       rabbitURL,
		QueueName: mainQueue,
		Durable:   true,
	}, logger)
	if err != nil {
		logger.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
	}
	defer client.Close()

	// Объявляем очереди (основную и DLQ)
	if err := client.DeclareDLQSetup(mainQueue, dlqName); err != nil {
		logger.Fatal("Failed to declare queues", zap.Error(err))
	}
	logger.Info("Queues declared successfully")

	// Устанавливаем prefetch для контроля нагрузки
	if err := client.SetPrefetch(prefetch); err != nil {
		logger.Fatal("Failed to set prefetch", zap.Error(err))
	}

	// Создаем хранилище обработанных сообщений (для идемпотентности)
	processedStore := store.NewProcessedStore()

	// Создаем процессор задач
	taskProcessor := processor.NewTaskProcessor(
		client,
		processedStore,
		processor.Config{
			MaxAttempts: maxAttempts,
			QueueName:   mainQueue,
			DLQName:     dlqName,
		},
		logger,
	)

	// Подписываемся на очередь
	deliveries, err := client.Consume(mainQueue)
	if err != nil {
		logger.Fatal("Failed to consume queue", zap.Error(err))
	}

	logger.Info("Worker started. Waiting for jobs...",
		zap.String("queue", mainQueue),
		zap.Int("max_attempts", maxAttempts))

	// Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сообщений в горутине
	go func() {
		for msg := range deliveries {
			taskProcessor.ProcessJob(ctx, msg)
		}
	}()

	// Ожидание сигнала завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down worker...")
}
