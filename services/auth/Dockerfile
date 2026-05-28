# BUILDER
FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/services/auth
RUN go build -o auth-service ./cmd/auth

# RUNNER
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/services/auth/auth-service .

EXPOSE 8081 50051

CMD ["./auth-service"]