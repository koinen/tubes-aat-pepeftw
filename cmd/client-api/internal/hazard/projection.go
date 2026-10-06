package hazard

import "fmt"

const (
	ClassSummary = "RINGKASAN"
	ClassRaw     = "MENTAH"
)

// AllFields maps every valid HazardView field to its classification.
var AllFields = map[string]string{
	// RINGKASAN
	"hazard_id":   ClassSummary,
	"source":      ClassSummary,
	"hazard_type": ClassSummary,
	"severity":    ClassSummary,
	"area_name":   ClassSummary,
	"occurred_at": ClassSummary,
	"ingested_at": ClassSummary,
	// MENTAH
	"source_ref_id": ClassRaw,
	"latitude":      ClassRaw,
	"longitude":     ClassRaw,
	"attributes":    ClassRaw,
}

const (
	ScopeSummary = "hazard:read:summary"
	ScopeRaw     = "hazard:read:raw"
)

// ValidateFields checks that every requested field is valid and accessible to scope.
// Returns errCode ("VALIDATION_ERROR" or "FORBIDDEN_FIELD") and errMsg, or empty strings if valid.
func ValidateFields(fields []string, scope string) (errCode, errMsg string) {
	for _, f := range fields {
		cls, ok := AllFields[f]
		if !ok {
			return "VALIDATION_ERROR", fmt.Sprintf("unknown field: %s", f)
		}
		if scope == ScopeSummary && cls == ClassRaw {
			return "FORBIDDEN_FIELD", fmt.Sprintf("Scope %s tidak mengizinkan field: %s", ScopeSummary, f)
		}
	}
	return "", ""
}

// DefaultFields returns the full set of allowed fields for a given scope.
func DefaultFields(scope string) []string {
	var out []string
	for f, cls := range AllFields {
		if scope == ScopeRaw || cls == ClassSummary {
			out = append(out, f)
		}
	}
	return out
}

// ProjectHazard retains only the specified fields in the hazard object.
func ProjectHazard(hazard map[string]any, fields []string) map[string]any {
	allowed := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		allowed[f] = struct{}{}
	}
	projected := make(map[string]any, len(fields))
	for k, v := range hazard {
		if _, ok := allowed[k]; ok {
			projected[k] = v
		}
	}
	return projected
}
