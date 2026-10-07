#!/usr/bin/env sh
set -u

cd "$(dirname "$0")/../.."

KEY=$(grep '^INTERNAL_API_KEY=' .env | cut -d= -f2-)
passed=0
failed=0

pass() { passed=$((passed + 1)); echo "  PASS: $*"; }
fail() { failed=$((failed + 1)); echo "  FAIL: $*"; }

newest_hazard() {
	curl -s -H "X-Internal-Key: $KEY" "http://localhost:8085/internal/hazards?limit=1" |
		grep -o '"hazard_id":"[^"]*"' | head -n 1 | cut -d'"' -f4
}
published_count() { docker compose logs aggregator 2>/dev/null | grep -c '"kafka_published"'; }
logs_have() { docker compose logs "$1" 2>/dev/null | grep -q "$2"; }
newer_than() { [ "$(newest_hazard)" != "$1" ]; }
published_more_than() { [ "$(published_count)" -gt "$1" ]; }
dashboard_has() { hurl --test --variable "hazard_id=$1" scripts/demo/p5-dashboard.hurl >/dev/null 2>&1; }
notifier_lag() {
	docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
		--describe --group notifier 2>/dev/null | awk '/hazard-events/ {sum += $6} END {print sum + 0}'
}
lag_is_zero() { [ "$(notifier_lag)" -eq 0 ]; }
aggregator_started() { docker inspect -f '{{.State.StartedAt}}' "$(docker compose ps -q aggregator)"; }
wait_until() {
	seconds=$1
	shift
	while [ "$seconds" -gt 0 ]; do
		"$@" && return 0
		sleep 2
		seconds=$((seconds - 2))
	done
	return 1
}

echo "5.1 New hazards are published to Kafka by the outbox relay"
count=$(published_count)
if [ "$count" -gt 0 ]; then pass "$count messages published so far"; else fail "no kafka_published lines in the aggregator log"; fi

echo "5.2 Two independent consumers receive the same hazard"
id=$(newest_hazard)
echo "  hazard: $id"
if wait_until 20 logs_have notifier "$id"; then pass "notifier received it"; else fail "notifier never logged it"; fi
if dashboard_has "$id"; then pass "dashboard received it"; else fail "dashboard never showed it"; fi

echo "5.3 Stop the notifier: the producer and the dashboard keep going"
docker compose stop notifier
before=$(published_count)
wait_until 40 newer_than "$id"
id2=$(newest_hazard)
echo "  hazard produced while the notifier is down: $id2"
if wait_until 20 published_more_than "$before"; then pass "relay kept publishing ($before -> $(published_count))"; else fail "relay stopped publishing"; fi
if dashboard_has "$id2"; then pass "dashboard still received new hazards"; else fail "dashboard stopped updating"; fi

echo "5.3 Start the notifier again: it catches up on what it missed"
docker compose start notifier
started=$(date +%s)
if wait_until 40 logs_have notifier "$id2"; then
	pass "notifier caught up on $id2 after $(($(date +%s) - started)) s"
else
	fail "notifier never processed $id2"
fi
if wait_until 30 lag_is_zero; then pass "notifier consumer-group lag is 0"; else fail "notifier lag stays at $(notifier_lag)"; fi

echo "5.4 Add a brand-new consumer without touching the producer"
docker compose --profile extra rm -sf audit-log >/dev/null 2>&1
group="audit-log-$(date +%s)"
echo "  new consumer group: $group"
aggregator_before=$(aggregator_started)
KAFKA_GROUP_ID_AUDIT="$group" docker compose --profile extra up -d audit-log
if wait_until 40 logs_have audit-log '"audit_recorded"'; then
	sleep 5
	pass "audit-log replayed the stream ($(docker compose logs audit-log 2>/dev/null | grep -c '"audit_recorded"') records so far)"
else
	fail "audit-log recorded nothing"
fi
if [ "$(aggregator_started)" = "$aggregator_before" ]; then pass "aggregator untouched (same start time)"; else fail "aggregator was restarted"; fi

echo "P5: $passed passed, $failed failed"
[ "$failed" -eq 0 ]
