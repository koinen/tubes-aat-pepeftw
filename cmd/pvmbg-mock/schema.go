package main

import (
	"encoding/json"
	"strconv"
)

// Schema versions of /volcanic-reports. A report keeps the version that was
// active when it was generated, so switching back does not repair old reports.
//
// Versions 1 and 2 are compatible changes. Versions 3 to 5 are breaking
// changes that build on version 2:
//   - 3 renames alert_level to alert_status.
//   - 4 sends eruption_count_24h as a string, for example "12".
//   - 5 drops the required field ash_column_height_m.
const (
	schemaBase            = 1
	schemaAdditive        = 2
	schemaRenameField     = 3
	schemaTypeChange      = 4
	schemaMissingRequired = 5
)

var schemaModes = map[int]string{
	schemaBase:            "base",
	schemaAdditive:        "additive_optional_field",
	schemaRenameField:     "rename_field",
	schemaTypeChange:      "type_change",
	schemaMissingRequired: "missing_required_field",
}

const schemaVersionHelp = "version harus 1 sampai 5: 1 base, 2 tambah confidence_level, " +
	"3 rename alert_level menjadi alert_status, 4 eruption_count_24h menjadi string, " +
	"5 ash_column_height_m hilang"

func validSchemaVersion(v int) bool {
	_, ok := schemaModes[v]
	return ok
}

func (r VolcanicReport) MarshalJSON() ([]byte, error) {
	type plain VolcanicReport
	b, err := json.Marshal(plain(r))
	if err != nil || r.schemaVersion < schemaRenameField {
		return b, err
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	switch r.schemaVersion {
	case schemaRenameField:
		m["alert_status"] = m["alert_level"]
		delete(m, "alert_level")
	case schemaTypeChange:
		m["eruption_count_24h"] = strconv.Itoa(r.EruptionCount24h)
	case schemaMissingRequired:
		delete(m, "ash_column_height_m")
	}
	return json.Marshal(m)
}
