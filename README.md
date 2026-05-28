## Практическая работа №14. Вуйко Ярослава, ЭФМО-01-25
### Реализация очереди задач (producer–consumer): retries, DLQ, идемпотентность. 28.05.2026


### Как поднят RabbitMQ и какие очереди созданы

RabbitMQ поднят через Docker Compose. Используется образ `rabbitmq:3.13-management-alpine`, который включает встроенный веб-интерфейс для мониторинга.

Порты:
- `5672` — AMQP (для подключения приложений)
- `15672` — Management UI (для администрирования)

Созданные очереди:
- `task_jobs` — основная очередь задач
- `task_jobs_dlq` — (Dead Letter Queue

Очереди создаются автоматически при запуске worker'а с помощью функции `DeclareDLQSetup`. Основная очередь настраивается с параметром `x-dead-letter-routing-key`, который указывает на DLQ. Это означает, что сообщения, которые не удалось обработать, автоматически отправляются в DLQ.

## Формат сообщения (JSON)

Задача, отправляемая в очередь, имеет следующую структуру:

```json
{
    "job": "process_task",
    "task_id": "t_fail",
    "attempt": 1,
    "message_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5"
}
```

Поля:
- `job` — тип задачи (process_task)
- `task_id` — идентификатор задачи в БД
- `attempt` — номер текущей попытки обработки (начинается с 1)
- `message_id` — уникальный UUID для идемпотентности

### Где и как публикуется сообщение

Сообщение публикуется сервисом `tasks` при вызове эндпоинта `/v1/jobs/process-task`. 

Порядок публикации:
1. Клиент отправляет POST-запрос с `task_id`
2. Сервис проверяет существование задачи в БД
3. Если задача существует, формируется `TaskJob` с `attempt=1` и новым `message_id`
4. Сообщение публикуется в очередь `task_jobs`

### Как устроен worker и где делается ack

Worker — отдельный сервис, который запускается в контейнере и подписывается на очередь `task_jobs`.

Логика обработки одного сообщения:

<img width="2374" height="5183" alt="2026-05-28" src="https://github.com/user-attachments/assets/efbbaf6a-9b92-4e6c-a3d6-137da6d83912" />


Где используется ack:
- После успешной обработки: `delivery.Ack(false)`
- После retry (публикации нового сообщения): `delivery.Ack(false)`
- После отправки в DLQ: `delivery.Ack(false)`

Ack означает: сообщение обработано (или перенаправлено), можно удалить из основной очереди. Если worker упадёт до ack, сообщение будет доставлено снова.

### Демонстрация

### Создание задачи
<img width="1372" height="662" alt="image" src="https://github.com/user-attachments/assets/642ad505-2a96-47bb-bfb1-00e73f09ffba" />


### Отправка задачи в очередь
<img width="1384" height="655" alt="image" src="https://github.com/user-attachments/assets/45497a48-6e90-4746-ae93-b61160508efe" />


### Отправка задачи в очередь(DLQ)
<img width="1384" height="655" alt="image" src="https://github.com/user-attachments/assets/57042308-79c5-4904-b8a5-fb16c5ce7fe7" />



### Логи worker'а

Успешная обработка (обычная задача):
```
2026-05-28T09:21:15.708Z        INFO    processor/processor.go:55       Processing job  {"component": "processor", "job_id": "4a6451e6-8149-4b8c-bbc0-46a7ecf81c61", "task_id": "t_4f6bbc99", "attempt": 1}
2026-05-28T09:21:17.710Z        INFO    processor/processor.go:75       Job processed successfully      {"component": "processor", "job_id": "4a6451e6-8149-4b8c-bbc0-46a7ecf81c61", "task_id": "t_4f6bbc99"}
```

Retry и DLQ (задача t_fail):
```
2026-05-28T09:21:59.209Z        INFO    processor/processor.go:55       Processing job  {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "task_id": "t_fail", "attempt": 1}
2026-05-28T09:22:01.211Z        WARN    processor/processor.go:83       Job processing failed   {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "attempt": 1, "error": "simulated processing error for task t_fail"}
worker-service/internal/processor.(*TaskProcessor).ProcessJob
        /app/services/worker/internal/processor/processor.go:83
main.main.func1
        /app/services/worker/cmd/worker/main.go:111
2026-05-28T09:22:01.212Z        DEBUG   rabbitmq/client.go:169  Job published   {"component": "rabbitmq", "queue": "task_jobs", "message_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5"}
2026-05-28T09:22:01.212Z        INFO    processor/processor.go:98       Job retried     {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "new_attempt": 2}
2026-05-28T09:22:01.213Z        INFO    processor/processor.go:55       Processing job  {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "task_id": "t_fail", "attempt": 2}
2026-05-28T09:22:03.214Z        WARN    processor/processor.go:83       Job processing failed   {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "attempt": 2, "error": "simulated processing error for task t_fail"}
worker-service/internal/processor.(*TaskProcessor).ProcessJob
        /app/services/worker/internal/processor/processor.go:83
main.main.func1
        /app/services/worker/cmd/worker/main.go:111
2026-05-28T09:22:03.214Z        DEBUG   rabbitmq/client.go:169  Job published   {"component": "rabbitmq", "queue": "task_jobs", "message_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5"}
2026-05-28T09:22:03.215Z        INFO    processor/processor.go:98       Job retried     {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "new_attempt": 3}
2026-05-28T09:22:03.221Z        INFO    processor/processor.go:55       Processing job  {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "task_id": "t_fail", "attempt": 3}
2026-05-28T09:22:05.223Z        WARN    processor/processor.go:83       Job processing failed   {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "attempt": 3, "error": "simulated processing error for task t_fail"}
worker-service/internal/processor.(*TaskProcessor).ProcessJob
        /app/services/worker/internal/processor/processor.go:83
main.main.func1
        /app/services/worker/cmd/worker/main.go:111
2026-05-28T09:22:05.223Z        WARN    processor/processor.go:107      Max attempts reached, sending to DLQ    {"component": "processor", "job_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5", "attempts": 3}
worker-service/internal/processor.(*TaskProcessor).ProcessJob
        /app/services/worker/internal/processor/processor.go:107
main.main.func1
        /app/services/worker/cmd/worker/main.go:111
2026-05-28T09:22:05.223Z        WARN    rabbitmq/client.go:203  Publishing to DLQ       {"component": "rabbitmq", "dlq": "task_jobs_dlq", "reason": "max_attempts_exceeded", "message_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5"}
tech-ip-sem2/shared/rabbitmq.(*Client).PublishToDLQ
        /app/shared/rabbitmq/client.go:203
worker-service/internal/processor.(*TaskProcessor).ProcessJob
        /app/services/worker/internal/processor/processor.go:111
main.main.func1
        /app/services/worker/cmd/worker/main.go:111
2026-05-28T09:22:05.223Z        INFO    rabbitmq/client.go:229  Message sent to DLQ     {"component": "rabbitmq", "dlq": "task_jobs_dlq"}
```

После трёх неудачных попыток сообщение попадает в очередь `task_jobs_dlq`.

### Демонстрация идемпотентности

В worker'е реализовано хранилище обработанных `message_id` (в памяти). При получении сообщения сначала проверяется, не обрабатывалось ли оно ранее. Это защищает от дублей при повторных доставках.

```go
if p.processedStore.IsProcessed(job.MessageID) {
    p.logger.Warn("Duplicate message detected, skipping")
    delivery.Ack(false)
    return
}
```

### Итог

В результате практического занятия реализована полностью рабочая очередь задач:

1. Созданы основная очередь task_jobs и очередь task_jobs_dlq с настройкой Dead Letter
2. Эндпоинт /v1/jobs/process-task публикует задачи в JSON-формате
3. Consumer (worker) читает очередь, обрабатывает задачи и отправляет ack
4. При ошибке выполняется до 3 попыток с увеличением счётчика
5. После 3 неудачных попыток сообщение отправляется в task_jobs_dlq
6. Хранилище message_id предотвращает повторную обработку


### Контрольные вопросы

1. Чем "job queue" отличается от "event queue"?
Job queue предназначена для выполнения конкретной работы, требует подтверждения и может пересылаться при ошибке. Event queue — это уведомление о том, что что-то произошло, обработка обычно лёгкая и не требует retry.

2. Почему система очередей часто работает как at-least-once?
Потому что ack может не успеть до падения consumer'а. Брокер не знает, обработалось сообщение или нет, и доставляет его снова. Гарантировать exactly-once сложнее и дороже.

3. Как DLQ помогает эксплуатации?
DLQ изолирует "плохие" сообщения, которые не получилось обработать. Они не блокируют основную очередь, их можно анализировать, исправлять ошибки и перезапускать вручную.

4. Почему ретраи нельзя делать бесконечно?
Бесконечные ретраи могут забить очередь, создавать бесконечную нагрузку на систему, маскировать реальные проблемы и откладывать ошибку, не давая понять, что что-то сломалось навсегда.

5. Что такое идемпотентность и как её реализовать минимально?
Идемпотентность — свойство операции, при котором повторное выполнение не меняет результат. Минимальная реализация: присвоить каждому сообщению уникальный `message_id` и хранить список уже обработанных ID, например в `map[string]bool`.




