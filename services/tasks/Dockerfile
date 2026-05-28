# BUILDER
FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/services/tasks
RUN go build -o tasks-service ./cmd/tasks

# RUNNER 
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/services/tasks/tasks-service .

EXPOSE 8082

CMD ["./tasks-service"]