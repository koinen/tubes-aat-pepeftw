#!/usr/bin/env bash
# Kafka smoke test: produce one message and consume it back through the broker.
set -euo pipefail

export MSYS_NO_PATHCONV=1

BROKER=localhost:9092
TOPIC=kafka-smoke
MARKER="smoke-$(date +%s)-$RANDOM"
BIN=/opt/kafka/bin

kexec() { docker compose exec -T kafka "$@"; }

echo "[1/3] ensuring topic $TOPIC exists"
kexec "$BIN/kafka-topics.sh" --bootstrap-server "$BROKER" \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null

echo "[2/3] producing $MARKER"
echo "$MARKER" | kexec "$BIN/kafka-console-producer.sh" --bootstrap-server "$BROKER" --topic "$TOPIC"

echo "[3/3] consuming it back"
if kexec "$BIN/kafka-console-consumer.sh" --bootstrap-server "$BROKER" --topic "$TOPIC" \
    --from-beginning --timeout-ms 10000 2>/dev/null | grep -q "$MARKER"; then
  echo "PASS: message round-tripped through Kafka"
else
  echo "FAIL: $MARKER was not consumed back" >&2
  exit 1
fi
