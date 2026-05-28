FROM golang:1.24-alpine AS builder

WORKDIR /app

# Копируем корневой go.mod (так как worker часть монорепозитория)
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь код
COPY . .

# Собираем worker
WORKDIR /app/services/worker
RUN go build -o worker-service ./cmd/worker

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/services/worker/worker-service .

CMD ["./worker-service"]