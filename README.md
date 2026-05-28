## Практическая работа №14. Вуйко Ярослава, ЭФМО-01-25
### Реализация очереди задач (producer–consumer): retries, DLQ, идемпотентность. 27.05.2026



### Очереди и их параметры

| Очередь | Тип | Durable | DLX параметры |
|---------|-----|---------|---------------|
| `task_jobs` | Основная | Да | `x-dead-letter-exchange: ""`, `x-dead-letter-routing-key: task_jobs_dlq` |
| `task_jobs_dlq` | DLQ | Да | Нет |


### Формат сообщения (задачи)

```json
{
    "job": "process_task",
    "task_id": "t_abc12345",
    "attempt": 1,
    "message_id": "fa9b9378-3c20-4ec2-ba04-7a6610e13ad5"
}
```

### Описание полей

| Поле | Тип | Описание |
|------|-----|----------|
| `job` | string | Тип задачи (`process_task`) |
| `task_id` | string | Идентификатор задачи в БД |
| `attempt` | int | Номер попытки обработки (начинается с 1) |
| `message_id` | string | Уникальный UUID для идемпотентности |

---

### Producer: публикация задач

### 5.1. Эндпоинт для создания задачи

```http
POST /v1/jobs/process-task
Authorization: Bearer demo-token
Content-Type: application/json

{
    "task_id": "t_fail"
}
```

### 5.2. Код публикации

```go
func (p *JobPublisher) PublishProcessTask(ctx context.Context, taskID string) error {
    messageID := uuid.New().String()
    
    job := &rabbitmq.TaskJob{
        Job:       "process_task",
        TaskID:    taskID,
        Attempt:   1,
        MessageID: messageID,
    }

    return p.rabbitClient.PublishJob(ctx, p.queueName, job)
}
```

---

## 6. Consumer: Worker с retry и DLQ

### 6.1. Создание очередей с DLQ

```go
func (c *Client) DeclareDLQSetup(mainQueue, dlqName string) error {
    // Объявляем DLQ
    if err := c.DeclareQueue(dlqName, true, ""); err != nil {
        return err
    }

    // Объявляем основную очередь с DLX
    if err := c.DeclareQueue(mainQueue, true, dlqName); err != nil {
        return err
    }
    return nil
}
```

### 6.2. Prefetch для контроля нагрузки

```go
// Устанавливаем prefetch = 1 (одно сообщение за раз)
client.SetPrefetch(1)
```

### 6.3. Алгоритм обработки сообщения

```go
func (p *TaskProcessor) ProcessJob(ctx context.Context, delivery amqp.Delivery) {
    // 1. Парсим задачу
    var job rabbitmq.TaskJob
    json.Unmarshal(delivery.Body, &job)

    // 2. Идемпотентность: проверяем, не обработано ли уже
    if p.processedStore.IsProcessed(job.MessageID) {
        delivery.Ack(false)
        return
    }

    // 3. Выполняем работу
    err := p.doWork(ctx, &job)

    // 4. Обрабатываем результат
    if err == nil {
        p.processedStore.MarkProcessed(job.MessageID)
        delivery.Ack(false)
        return
    }

    // 5. Retry логика
    if job.Attempt < p.config.MaxAttempts {
        job.Attempt++
        p.rabbitClient.PublishJob(ctx, p.config.QueueName, &job)
        delivery.Ack(false) // подтверждаем исходное сообщение
        return
    }

    // 6. Превышено число попыток → отправляем в DLQ
    p.rabbitClient.PublishToDLQ(ctx, p.config.DLQName, delivery, "max_attempts_exceeded")
    delivery.Ack(false)
}
```

### 6.4. Симуляция ошибок (для демонстрации)

```go
func (p *TaskProcessor) doWork(ctx context.Context, job *rabbitmq.TaskJob) error {
    // Имитация длительной обработки (2 секунды)
    time.Sleep(2 * time.Second)

    // Симуляция ошибки для task_id = "t_fail"
    if job.TaskID == "t_fail" {
        return fmt.Errorf("simulated processing error for task %s", job.TaskID)
    }

    return nil
}
```

---

## 7. Идемпотентность

Для защиты от дублирования сообщений используется хранилище обработанных `message_id`:

```go
type ProcessedStore struct {
    mu        sync.RWMutex
    processed map[string]bool
}

func (s *ProcessedStore) IsProcessed(messageID string) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.processed[messageID]
}

func (s *ProcessedStore) MarkProcessed(messageID string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.processed[messageID] = true
}
```

---

## 8. Демонстрация работы

### 8.1. Запуск системы

```powershell
cd deploy/tls
docker-compose up --build -d
```

### 8.2. Создание задачи (успешная обработка)

```powershell
# Создаём задачу в БД
docker exec -it postgres-db psql -U postgres -d tasks_db -c "
INSERT INTO tasks (id, title, description, due_date, done, created_at, updated_at)
VALUES ('t_4f6bbc99', 'Test task', 'Success', '2026-12-31', false, NOW(), NOW());
"

# Отправляем в очередь
curl -k -X POST https://localhost:8443/v1/jobs/process-task `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer demo-token" `
  -d '{\"task_id\":\"t_4f6bbc99\"}'
```

**Логи worker:**
```
INFO Processing job {"task_id": "t_4f6bbc99", "attempt": 1}
INFO Job processed successfully
```

### 8.3. Создание задачи с ошибкой (retry и DLQ)

```powershell
curl -k -X POST https://localhost:8443/v1/jobs/process-task `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer demo-token" `
  -d '{\"task_id\":\"t_fail\"}'
```

**Логи worker (попытка 1):**
```
INFO Processing job {"task_id": "t_fail", "attempt": 1}
WARN Job processing failed {"error": "simulated processing error for task t_fail"}
INFO Job retried {"new_attempt": 2}
```

**Логи worker (попытка 2):**
```
INFO Processing job {"task_id": "t_fail", "attempt": 2}
WARN Job processing failed
INFO Job retried {"new_attempt": 3}
```

**Логи worker (попытка 3 → DLQ):**
```
INFO Processing job {"task_id": "t_fail", "attempt": 3}
WARN Job processing failed
WARN Max attempts reached, sending to DLQ
INFO Message sent to DLQ {"dlq": "task_jobs_dlq"}
```

### 8.4. Проверка DLQ

```powershell
# Проверка очереди DLQ через API
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/task_jobs_dlq | ConvertFrom-Json | Select-Object name, messages_ready
```

Результат:
```
name            messages_ready
----            --------------
task_jobs_dlq   1
```

---

## 9. Логи успешного запуска worker

```
INFO Worker configuration {"rabbit_url": "amqp://guest:guest@rabbitmq:5672/", "main_queue": "task_jobs", "dlq_name": "task_jobs_dlq", "max_attempts": 3}
DEBUG Queue declared {"queue": "task_jobs_dlq", "args": {}}
DEBUG Queue declared {"queue": "task_jobs", "args": {"x-dead-letter-exchange":"","x-dead-letter-routing-key":"task_jobs_dlq"}}
INFO DLQ setup completed {"main_queue": "task_jobs", "dlq": "task_jobs_dlq"}
INFO Queues declared successfully
INFO Worker started. Waiting for jobs...
```

---

## 10. Скриншоты из RabbitMQ Management UI

### 10.1. Очередь task_jobs с DLQ параметрами

*На скриншоте видно: очередь `task_jobs` с параметром `x-dead-letter-routing-key: task_jobs_dlq`*

### 10.2. Очередь task_jobs_dlq с сообщением

*На скриншоте видно: 1 сообщение в DLQ после 3 неудачных попыток*

---

## 11. Контрольные вопросы

**1. Что такое DLQ и зачем она нужна?**

DLQ (Dead Letter Queue) — очередь "мёртвых" сообщений, куда попадают сообщения, которые не удалось обработать после всех повторных попыток. Она нужна, чтобы не терять "плохие" сообщения и иметь возможность их проанализировать или вручную переобработать.

**2. Как работает retry в вашей реализации?**

При ошибке обработки увеличивается счётчик `attempt`. Если `attempt < max_attempts`, сообщение публикуется заново в основную очередь. Исходное сообщение подтверждается (ack), чтобы не зацикливаться. После достижения `max_attempts` сообщение отправляется в DLQ.

**3. Что такое prefetch и почему он важен?**

Prefetch ограничивает количество сообщений, которые consumer может получить до отправки ack. Он важен для контроля нагрузки: если обработка тяжёлая, prefetch=1 гарантирует, что worker не возьмёт больше сообщений, чем может обработать.

**4. Как реализована идемпотентность?**

Каждое сообщение имеет уникальный `message_id`. Worker хранит в памяти (map) уже обработанные ID. При получении сообщения проверяется, не обрабатывался ли уже этот ID. Если да — сообщение подтверждается, но работа не выполняется повторно.

**5. Почему сообщение может быть доставлено повторно?**

Из-за at-least-once доставки. Если worker упал после выполнения работы, но до отправки ack, брокер переотправит сообщение другому consumer'у. Идемпотентность защищает от дублирования в таких случаях.

---

## 12. Вывод

В результате практического занятия успешно реализована очередь задач с поддержкой:

| Функция | Реализация |
|---------|------------|
| **Retries** | 3 попытки с логированием каждой |
| **DLQ** | Очередь `task_jobs_dlq` для сообщений, не прошедших обработку |
| **Prefetch** | `prefetch=1` для контроля нагрузки |
| **Идемпотентность** | Хранилище обработанных `message_id` |
| **Отказоустойчивость** | RabbitMQ с durable очередями |

Система готова к использованию в production-сценариях с гарантированной доставкой и обработкой ошибок.
