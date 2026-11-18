package mapper

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// VolcanicAdaptations are the adaptation rules a human declared for PVMBG volcanic reports
// (PVMBG_ADAPTATIONS). The mapper applies only these rules; it never guesses.
type VolcanicAdaptations struct {
	rules    []adaptationRule
	observer func(rule, sourceRefID string)
}

// WithObserver returns the same rules, reporting each rule a report needed once that report has
// become a HazardEvent. The observer is not part of the configuration and does not change String.
func (a VolcanicAdaptations) WithObserver(fn func(rule, sourceRefID string)) VolcanicAdaptations {
	a.observer = fn
	return a
}

type adaptationRule struct {
	kind  string
	field string // the field the rule acts on; for rename, the incoming name
	to    string // rename only: the canonical name
}

const (
	ruleRename        = "rename"
	ruleNumericString = "numeric_string"
	ruleOptional      = "optional"
)

// volcanicFields are the fields Volcanic reads. The value says whether the field is a number that
// the mapper only checks (and keeps in attributes); only those may be numeric_string or optional.
var volcanicFields = map[string]bool{
	"report_id":           false,
	"volcano_id":          false,
	"alert_level":         false,
	"reported_at":         false,
	"eruption_count_24h":  true,
	"ash_column_height_m": true,
}

func (r adaptationRule) String() string {
	if r.kind == ruleRename {
		return r.kind + ":" + r.field + "=" + r.to
	}
	return r.kind + ":" + r.field
}

// ParseVolcanicAdaptations reads a comma-separated rule list. An empty string means no rules.
// A rule the mapper cannot apply exactly is an error, so a typo stops the pod from starting.
func ParseVolcanicAdaptations(s string) (VolcanicAdaptations, error) {
	var a VolcanicAdaptations
	seen := map[string]bool{}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		r, err := parseRule(item)
		if err != nil {
			return VolcanicAdaptations{}, fmt.Errorf("PVMBG_ADAPTATIONS %q: %w", item, err)
		}
		// A field may be renamed once, a canonical name may be the target of one rename,
		// and a field may carry each other rule kind once.
		keys := []string{r.kind + ":" + r.field}
		if r.kind == ruleRename {
			keys = append(keys, "rename-to:"+r.to)
		}
		for _, k := range keys {
			if seen[k] {
				return VolcanicAdaptations{}, fmt.Errorf("PVMBG_ADAPTATIONS %q: conflicts with an earlier rule", item)
			}
			seen[k] = true
		}
		a.rules = append(a.rules, r)
	}
	return a, nil
}

func parseRule(item string) (adaptationRule, error) {
	kind, arg, ok := strings.Cut(item, ":")
	if !ok {
		return adaptationRule{}, fmt.Errorf("want <kind>:<field>")
	}
	r := adaptationRule{kind: kind, field: arg}
	switch kind {
	case ruleRename:
		var hasTo bool
		r.field, r.to, hasTo = strings.Cut(arg, "=")
		if !hasTo || r.field == "" || r.to == "" {
			return r, fmt.Errorf("want rename:<incoming>=<canonical>")
		}
		if _, known := volcanicFields[r.to]; !known {
			return r, fmt.Errorf("%s is not a field the PVMBG mapper reads", r.to)
		}
		if _, known := volcanicFields[r.field]; known {
			return r, fmt.Errorf("%s is already a field the PVMBG mapper reads", r.field)
		}
	case ruleNumericString, ruleOptional:
		numeric, known := volcanicFields[r.field]
		if !known {
			return r, fmt.Errorf("%q is not a field the PVMBG mapper reads", r.field)
		}
		if !numeric {
			return r, fmt.Errorf("%s applies only to eruption_count_24h and ash_column_height_m", kind)
		}
	default:
		return r, fmt.Errorf("unknown rule kind %q (want rename, numeric_string, or optional)", kind)
	}
	return r, nil
}

// String is the canonical form of the rules, used to compare two pods' configuration.
func (a VolcanicAdaptations) String() string {
	parts := make([]string, len(a.rules))
	for i, r := range a.rules {
		parts[i] = r.String()
	}
	return strings.Join(parts, ",")
}

// apply rewrites the decoded report according to the rules and returns the rules that acted. A rule
// acts only when its field is present. A report carrying both the old and the new name of a renamed
// field is rejected: picking one is a guess.
func (a VolcanicAdaptations) apply(f *fields) ([]adaptationRule, error) {
	var acted []adaptationRule
	for _, r := range a.rules {
		v, ok := f.m[r.field]
		if !ok {
			continue
		}
		switch r.kind {
		case ruleRename:
			if _, both := f.m[r.to]; both {
				ref, _ := f.m["report_id"].(string)
				return nil, reject(ref, "field %s dan %s sama-sama ada, aturan rename tidak bisa memilih", r.field, r.to)
			}
			f.m[r.to] = v
			delete(f.m, r.field)
			acted = append(acted, r)
		case ruleNumericString:
			if n, ok := numberText(v); ok {
				f.m[r.field] = n
				acted = append(acted, r)
			}
		}
	}
	return acted, nil
}

// jsonNumber is the JSON number grammar. numeric_string accepts exactly the text a source would
// have sent unquoted, nothing looser (no spaces, hex, "NaN", or "Inf").
var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// numberText returns the number a string holds. Anything else is left for the mapper to reject.
func numberText(v any) (float64, bool) {
	s, ok := v.(string)
	if !ok || !jsonNumber.MatchString(s) {
		return 0, false
	}
	n, err := strconv.ParseFloat(s, 64)
	return n, err == nil
}

// optional returns the rule that declares a missing or null field acceptable, if there is one.
func (a VolcanicAdaptations) optional(field string) (adaptationRule, bool) {
	for _, r := range a.rules {
		if r.kind == ruleOptional && r.field == field {
			return r, true
		}
	}
	return adaptationRule{}, false
}

func (a VolcanicAdaptations) notify(acted []adaptationRule, sourceRefID string) {
	if a.observer == nil {
		return
	}
	for _, r := range acted {
		a.observer(r.String(), sourceRefID)
	}
}
