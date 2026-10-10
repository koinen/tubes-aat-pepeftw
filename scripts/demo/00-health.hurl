# Aggregator: database and Kafka reachable
GET http://localhost:8085/health
[Options]
retry: 40
retry-interval: 3s
HTTP 200
[Asserts]
jsonpath "$.checks.postgres" == "ok"
jsonpath "$.checks.kafka" == "ok"

# Client API
GET http://localhost:8080/health
[Options]
retry: 40
retry-interval: 3s
HTTP 200

# Auth
GET http://localhost:8084/health
[Options]
retry: 40
retry-interval: 3s
HTTP 200

# BMKG mock
GET http://localhost:8081/health
[Options]
retry: 40
retry-interval: 3s
HTTP 200

# PVMBG mock
GET http://localhost:8082/health
[Options]
retry: 40
retry-interval: 3s
HTTP 200

# Dashboard
GET http://localhost:8083/health
[Options]
retry: 40
retry-interval: 3s
HTTP 200
