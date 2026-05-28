module worker-service

go 1.24.4

require (
	github.com/rabbitmq/amqp091-go v1.11.0
	go.uber.org/zap v1.28.0
	tech-ip-sem2 v0.0.0-00010101000000-000000000000
)

require go.uber.org/multierr v1.11.0 // indirect

replace tech-ip-sem2 => ../../
