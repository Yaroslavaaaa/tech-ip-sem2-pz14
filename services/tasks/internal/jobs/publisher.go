package jobs

import (
	"context"
	"fmt"

	"tech-ip-sem2/shared/rabbitmq"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type JobPublisher struct {
	rabbitClient *rabbitmq.Client
	queueName    string
	logger       *zap.Logger
}

func NewJobPublisher(rabbitClient *rabbitmq.Client, queueName string, logger *zap.Logger) *JobPublisher {
	return &JobPublisher{
		rabbitClient: rabbitClient,
		queueName:    queueName,
		logger:       logger.With(zap.String("component", "job_publisher")),
	}
}

// PublishProcessTask публикует задачу на обработку задачи
func (p *JobPublisher) PublishProcessTask(ctx context.Context, taskID string) error {
	messageID := uuid.New().String()

	job := &rabbitmq.TaskJob{
		Job:       "process_task",
		TaskID:    taskID,
		Attempt:   1,
		MessageID: messageID,
	}

	p.logger.Debug("Publishing job",
		zap.String("task_id", taskID),
		zap.String("message_id", messageID))

	if err := p.rabbitClient.PublishJob(ctx, p.queueName, job); err != nil {
		return fmt.Errorf("failed to publish job: %w", err)
	}

	return nil
}
