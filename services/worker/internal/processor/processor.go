package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"worker-service/internal/store"

	"go.uber.org/zap"

	"tech-ip-sem2/shared/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	MaxAttempts int
	QueueName   string
	DLQName     string
}

type TaskProcessor struct {
	config         Config
	rabbitClient   *rabbitmq.Client
	processedStore *store.ProcessedStore
	logger         *zap.Logger
}

func NewTaskProcessor(
	rabbitClient *rabbitmq.Client,
	processedStore *store.ProcessedStore,
	config Config,
	logger *zap.Logger,
) *TaskProcessor {
	return &TaskProcessor{
		config:         config,
		rabbitClient:   rabbitClient,
		processedStore: processedStore,
		logger:         logger.With(zap.String("component", "processor")),
	}
}

// ProcessJob обрабатывает одно сообщение из очереди
func (p *TaskProcessor) ProcessJob(ctx context.Context, delivery amqp.Delivery) {
	var job rabbitmq.TaskJob
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		p.logger.Error("Failed to parse job", zap.Error(err))
		delivery.Nack(false, false)
		return
	}

	p.logger.Info("Processing job",
		zap.String("job_id", job.MessageID),
		zap.String("task_id", job.TaskID),
		zap.Int("attempt", job.Attempt))

	if p.processedStore.IsProcessed(job.MessageID) {
		p.logger.Warn("Duplicate message detected, skipping",
			zap.String("message_id", job.MessageID))
		delivery.Ack(false)
		return
	}

	err := p.doWork(ctx, &job)

	if err == nil {
		p.processedStore.MarkProcessed(job.MessageID)
		p.logger.Info("Job processed successfully",
			zap.String("job_id", job.MessageID),
			zap.String("task_id", job.TaskID))
		delivery.Ack(false)
		return
	}

	p.logger.Warn("Job processing failed",
		zap.String("job_id", job.MessageID),
		zap.Int("attempt", job.Attempt),
		zap.Error(err))

	if job.Attempt < p.config.MaxAttempts {
		job.Attempt++

		if pubErr := p.rabbitClient.PublishJob(ctx, p.config.QueueName, &job); pubErr != nil {
			p.logger.Error("Failed to retry job", zap.Error(pubErr))
			delivery.Nack(false, false)
		} else {
			p.logger.Info("Job retried",
				zap.String("job_id", job.MessageID),
				zap.Int("new_attempt", job.Attempt))
			delivery.Ack(false)
		}
		return
	}

	p.logger.Warn("Max attempts reached, sending to DLQ",
		zap.String("job_id", job.MessageID),
		zap.Int("attempts", job.Attempt))

	if dlqErr := p.rabbitClient.PublishToDLQ(ctx, p.config.DLQName, delivery, "max_attempts_exceeded"); dlqErr != nil {
		p.logger.Error("Failed to publish to DLQ", zap.Error(dlqErr))
		delivery.Nack(false, false)
	} else {
		delivery.Ack(false)
	}
}

// doWork выполняет фактическую работу (с симуляцией ошибок)
func (p *TaskProcessor) doWork(ctx context.Context, job *rabbitmq.TaskJob) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}

	if job.TaskID == "t_fail" || job.TaskID == "t_error" {
		return fmt.Errorf("simulated processing error for task %s", job.TaskID)
	}

	if time.Now().UnixNano()%10 < 3 {
		return fmt.Errorf("random error for task %s", job.TaskID)
	}

	return nil
}
