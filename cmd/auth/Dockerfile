# Stage 1: Build the Go application
FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# avoid copying other services and files that are not needed for the build
COPY internal/ internal/
COPY cmd/auth/ cmd/auth/
RUN CGO_ENABLED=0 GOOS=linux go build -o ./bin/auth ./cmd/auth

# Stage 2: Create a minimal image for the application
FROM alpine:latest

WORKDIR /root/
COPY --from=builder /app/bin/auth .

CMD ["./auth"]