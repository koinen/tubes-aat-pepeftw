# 3.1 Control: each agency accepts its own credential
GET http://localhost:8081/seismic-events
X-BMKG-Key: {{BMKG_API_KEY}}
HTTP 200

GET http://localhost:8082/volcanic-reports
Authorization: Bearer {{PVMBG_TOKEN}}
HTTP 200

# 3.1 A PVMBG credential is rejected by BMKG
GET http://localhost:8081/seismic-events
Authorization: Bearer {{PVMBG_TOKEN}}
HTTP *
[Asserts]
status >= 401
status <= 403

# 3.1 A BMKG credential is rejected by PVMBG
GET http://localhost:8082/volcanic-reports
X-BMKG-Key: {{BMKG_API_KEY}}
HTTP *
[Asserts]
status >= 401
status <= 403

# 3.4 A wrong client secret is rejected
POST http://localhost:8084/token
{"grant_type": "client_credentials", "client_id": "media", "client_secret": "wrong-secret"}
HTTP 401

# 3.4 Three separate identities with their own scopes
POST http://localhost:8084/token
{"grant_type": "client_credentials", "client_id": "media", "client_secret": "{{MEDIA_SECRET}}"}
HTTP 200
[Captures]
media: jsonpath "$.access_token"
[Asserts]
jsonpath "$.scope" == "hazard:read:summary"
jsonpath "$.expires_in" == 60

POST http://localhost:8084/token
{"grant_type": "client_credentials", "client_id": "field-team", "client_secret": "{{LAPANGAN_SECRET}}"}
HTTP 200
[Captures]
field: jsonpath "$.access_token"
field_refresh: jsonpath "$.refresh_token"
[Asserts]
jsonpath "$.scope" == "hazard:read:raw"

POST http://localhost:8084/token
{"grant_type": "client_credentials", "client_id": "bnpb-ops", "client_secret": "{{BNPB_SECRET}}"}
HTTP 200
[Asserts]
jsonpath "$.scope" == "hazard:read:raw"

# 3.2 Media only receives summary fields
GET http://localhost:8080/hazards?limit=3
Authorization: Bearer {{media}}
HTTP 200
[Asserts]
jsonpath "$.data[0].hazard_id" exists
jsonpath "$.data[0].severity" exists
jsonpath "$.data[0].latitude" not exists
jsonpath "$.data[0].longitude" not exists
jsonpath "$.data[0].attributes" not exists
jsonpath "$.data[0].source_ref_id" not exists

# 3.3 Media explicitly asking for raw fields is refused by the server
GET http://localhost:8080/hazards?fields=latitude
Authorization: Bearer {{media}}
HTTP 403
[Asserts]
jsonpath "$.error.code" == "FORBIDDEN_FIELD"

GET http://localhost:8080/hazards?fields=attributes
Authorization: Bearer {{media}}
HTTP 403
[Asserts]
jsonpath "$.error.code" == "FORBIDDEN_FIELD"

# Field team has the raw scope and sees raw fields
GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{field}}
HTTP 200
[Asserts]
jsonpath "$.data[0].latitude" exists
jsonpath "$.data[0].attributes" exists

# 3.5 Mid-session the field team's token expires naturally after its 60 s TTL
GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{field}}
[Options]
delay: 61s
HTTP 401

# 3.5 Refresh continues the session without a new login
POST http://localhost:8084/token
{"grant_type": "refresh_token", "refresh_token": "{{field_refresh}}"}
HTTP 200
[Captures]
field2: jsonpath "$.access_token"
field2_refresh: jsonpath "$.refresh_token"

GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{field2}}
HTTP 200

# 3.5 Refreshing again while the token is still valid revokes the old token (revocation travels through Kafka)
POST http://localhost:8084/token
{"grant_type": "refresh_token", "refresh_token": "{{field2_refresh}}"}
HTTP 200
[Captures]
field3: jsonpath "$.access_token"
field3_refresh: jsonpath "$.refresh_token"

GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{field2}}
[Options]
retry: 10
retry-interval: 1s
HTTP 401

GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{field3}}
HTTP 200

# 3.5 Reusing an old refresh token is detected and kills the whole session
POST http://localhost:8084/token
{"grant_type": "refresh_token", "refresh_token": "{{field2_refresh}}"}
HTTP 401

GET http://localhost:8080/hazards?limit=1
Authorization: Bearer {{field3}}
[Options]
retry: 10
retry-interval: 1s
HTTP 401

POST http://localhost:8084/token
{"grant_type": "refresh_token", "refresh_token": "{{field3_refresh}}"}
HTTP 401
