# Stage 1: Build the Go application
FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o ./bin/bmkg-mock ./cmd/bmkg-mock

# Stage 2: Create a minimal image for the application
FROM alpine:latest

WORKDIR /root/
COPY --from=builder /app/bin/bmkg-mock .

CMD ["./bmkg-mock"]