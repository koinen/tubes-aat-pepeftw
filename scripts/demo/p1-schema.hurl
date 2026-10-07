# 1.0 Start from the original PVMBG schema
POST http://localhost:8082/admin/schema-version
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"version": 1}
HTTP 200
[Asserts]
jsonpath "$.schema_version" == 1

# 1.1 BMKG SeismicEvent mapped to HazardEvent
GET http://localhost:8085/internal/hazards?type=SEISMIC&limit=5
X-Internal-Key: {{INTERNAL_API_KEY}}
[Options]
retry: 20
retry-interval: 3s
HTTP 200
[Asserts]
jsonpath "$.data" count > 0
jsonpath "$.data[0].source" == "BMKG"
jsonpath "$.data[0].hazard_type" == "SEISMIC"
jsonpath "$.data[0].severity" matches /^(NORMAL|WASPADA|SIAGA|AWAS)$/
jsonpath "$.data[0].attributes.magnitude" exists
jsonpath "$.data[0].attributes.depth_km" exists

# 1.1 PVMBG VolcanicReport mapped to HazardEvent, original schema has no confidence_level
GET http://localhost:8085/internal/hazards?type=VOLCANIC&limit=1
X-Internal-Key: {{INTERNAL_API_KEY}}
[Options]
retry: 20
retry-interval: 3s
HTTP 200
[Captures]
old_id: jsonpath "$.data[0].hazard_id"
[Asserts]
jsonpath "$.data" count == 1
jsonpath "$.data[0].source" == "PVMBG"
jsonpath "$.data[0].hazard_type" == "VOLCANIC"
jsonpath "$.data[0].area_name" exists
jsonpath "$.data[0].attributes.volcano_id" exists
jsonpath "$.data[0].attributes.eruption_count_24h" exists
jsonpath "$.data[0].attributes.confidence_level" not exists

# 1.2 PVMBG schema change while everything keeps running
POST http://localhost:8082/admin/schema-version
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"version": 2}
HTTP 200
[Asserts]
jsonpath "$.schema_version" == 2

# 1.3 New reports still produce valid HazardEvents, the unknown field lands in attributes
GET http://localhost:8085/internal/hazards?type=VOLCANIC&limit=1
X-Internal-Key: {{INTERNAL_API_KEY}}
[Options]
retry: 20
retry-interval: 3s
HTTP 200
[Asserts]
jsonpath "$.data[0].source" == "PVMBG"
jsonpath "$.data[0].attributes.confidence_level" exists
jsonpath "$.data[0].attributes.confidence_level" >= 0
jsonpath "$.data[0].attributes.confidence_level" <= 1
jsonpath "$.data[0].confidence_level" not exists

# 1.3 The record from before the change is still readable next to the new one
GET http://localhost:8085/internal/hazards/{{old_id}}
X-Internal-Key: {{INTERNAL_API_KEY}}
HTTP 200
[Asserts]
jsonpath "$.data.hazard_id" == "{{old_id}}"
jsonpath "$.data.attributes.confidence_level" not exists

# 1.4 The aggregator never crashed
GET http://localhost:8085/health
HTTP 200
[Asserts]
jsonpath "$.status" == "ok"

# Reset so the demo can run again
POST http://localhost:8082/admin/schema-version
X-Admin-Key: {{PVMBG_ADMIN_KEY}}
{"version": 1}
HTTP 200
