FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/field-team-receiver/ cmd/field-team-receiver/
RUN CGO_ENABLED=0 GOOS=linux go build -o ./bin/field-team-receiver ./cmd/field-team-receiver

FROM alpine:3.24.2

WORKDIR /root/
COPY --from=builder /app/bin/field-team-receiver .

CMD ["./field-team-receiver"]
