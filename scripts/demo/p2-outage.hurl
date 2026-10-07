# 2.0 Start with PVMBG healthy
POST http://localhost:8082/admin/outage
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"enabled": false}
HTTP 200

GET http://localhost:8085/internal/sources/status
X-Internal-Key: {{INTERNAL_API_KEY}}
[Options]
retry: 20
retry-interval: 3s
HTTP 200
[Asserts]
jsonpath "$.data.PVMBG.status" == "OK"

# Token for an operations client
POST http://localhost:8084/token
{"grant_type": "client_credentials", "client_id": "bnpb-ops", "client_secret": "{{BNPB_SECRET}}"}
HTTP 200
[Captures]
token: jsonpath "$.access_token"

# 2.3 PVMBG goes down (requests hang)
POST http://localhost:8082/admin/outage
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"enabled": true, "mode": "hang"}
HTTP 200
[Asserts]
jsonpath "$.outage.enabled" == true

GET http://localhost:8085/internal/sources/status
X-Internal-Key: {{INTERNAL_API_KEY}}
[Options]
retry: 20
retry-interval: 3s
HTTP 200
[Asserts]
jsonpath "$.data.PVMBG.status" == "STALE"
jsonpath "$.data.PVMBG.stale_since" exists
jsonpath "$.data.BMKG.status" == "OK"

# 2.3 Seismic data is still served normally
GET http://localhost:8080/hazards?type=SEISMIC&limit=5
Authorization: Bearer {{token}}
HTTP 200
[Asserts]
duration < 1000
jsonpath "$.data" count > 0

# 2.3 Volcanic data: last known data plus stale_since, no 500, no hang
GET http://localhost:8080/hazards?type=VOLCANIC&limit=5
Authorization: Bearer {{token}}
HTTP 200
[Asserts]
duration < 2000
jsonpath "$.data" count > 0
jsonpath "$.sources.PVMBG.status" == "STALE"
jsonpath "$.sources.PVMBG.stale_since" exists

# 2.3 Same when PVMBG answers with errors instead of hanging
POST http://localhost:8082/admin/outage
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"enabled": true, "mode": "error"}
HTTP 200
[Asserts]
jsonpath "$.outage.mode" == "error"

GET http://localhost:8080/hazards?type=VOLCANIC&limit=5
Authorization: Bearer {{token}}
HTTP 200
[Asserts]
duration < 2000
jsonpath "$.sources.PVMBG.stale_since" exists

# 2.4 PVMBG comes back, the system recovers on its own
POST http://localhost:8082/admin/outage
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"enabled": false}
HTTP 200

GET http://localhost:8085/internal/sources/status
X-Internal-Key: {{INTERNAL_API_KEY}}
[Options]
retry: 20
retry-interval: 3s
HTTP 200
[Asserts]
jsonpath "$.data.PVMBG.status" == "OK"
jsonpath "$.data.PVMBG.stale_since" == null

GET http://localhost:8080/sources/status
Authorization: Bearer {{token}}
HTTP 200
[Asserts]
jsonpath "$.data.PVMBG.status" == "OK"
