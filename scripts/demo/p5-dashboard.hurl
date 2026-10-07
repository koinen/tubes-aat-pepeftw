# The dashboard consumer received this hazard
GET http://localhost:8083/api/hazards
[Options]
retry: 15
retry-interval: 2s
HTTP 200
[Asserts]
jsonpath "$.data[*].hazard_id" includes "{{hazard_id}}"
