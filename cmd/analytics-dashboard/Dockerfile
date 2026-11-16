FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/analytics-dashboard/ cmd/analytics-dashboard/
RUN CGO_ENABLED=0 GOOS=linux go build -o ./bin/analytics-dashboard ./cmd/analytics-dashboard

FROM alpine:3.24.2

WORKDIR /root/
COPY --from=builder /app/bin/analytics-dashboard .

CMD ["./analytics-dashboard"]
