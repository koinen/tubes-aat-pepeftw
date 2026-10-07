# Token for the media client
POST http://localhost:8084/token
{"grant_type": "client_credentials", "client_id": "media", "client_secret": "{{MEDIA_SECRET}}"}
HTTP 200
[Captures]
token: jsonpath "$.access_token"

# Controlled rejection: keep calling until the per-client limit answers 429
GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{token}}
[Options]
retry: 30
retry-interval: 0
HTTP 429
[Asserts]
header "Retry-After" exists
jsonpath "$.error.code" exists
