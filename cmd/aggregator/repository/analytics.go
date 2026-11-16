package repository

import (
	"context"
	"time"
)

const (
	CategorySeismic  = "SEISMIC"
	CategoryTsunami  = "TSUNAMI"
	CategoryVolcanic = "VOLCANIC"
	CategoryWildfire = "WILDFIRE"
	CategoryCoastal  = "COASTAL"
)

var AnalyticsBuckets = map[string]time.Duration{
	"hour":  time.Hour,
	"day":   24 * time.Hour,
	"month": 30 * 24 * time.Hour,
	"year":  365 * 24 * time.Hour,
}

const MaxAnalyticsBuckets = 2000

type AnalyticsFilter struct {
	From   time.Time
	To     time.Time
	Bucket string
}

type SeriesPoint struct {
	Bucket   time.Time
	Category string
	Count    int
}

type NamedCount struct {
	Name  string
	Count int
}

type Analytics struct {
	Total            int
	Series           []SeriesPoint
	BySeverity       []NamedCount
	ByArea           []NamedCount
	AsOf             time.Time
	Partial          bool
	UnavailableUnits []string
}

type AnalyticsReader interface {
	Analytics(ctx context.Context, f AnalyticsFilter) (Analytics, error)
}
