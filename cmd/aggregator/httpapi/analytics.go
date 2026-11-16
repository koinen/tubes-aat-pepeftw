package httpapi

import (
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type seriesPointResponse struct {
	Bucket   time.Time `json:"bucket"`
	Category string    `json:"category"`
	Count    int       `json:"count"`
}

type namedCountResponse struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type analyticsResponse struct {
	From             time.Time             `json:"from"`
	To               time.Time             `json:"to"`
	Bucket           string                `json:"bucket"`
	Total            int                   `json:"total"`
	Series           []seriesPointResponse `json:"series"`
	BySeverity       []namedCountResponse  `json:"by_severity"`
	ByArea           []namedCountResponse  `json:"by_area"`
	AsOf             string                `json:"as_of"`
	Partial          bool                  `json:"partial"`
	UnavailableUnits []string              `json:"unavailable_units"`
}

func (h handlers) analytics(c fiber.Ctx) error {
	f, err := parseAnalyticsFilter(c)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, codeValidation, err.Error())
	}
	a, err := h.analyticsRepo.Analytics(c.Context(), f)
	if err != nil {
		return err
	}

	resp := analyticsResponse{
		From: f.From, To: f.To, Bucket: f.Bucket, Total: a.Total,
		Series:           make([]seriesPointResponse, 0, len(a.Series)),
		BySeverity:       named(a.BySeverity),
		ByArea:           named(a.ByArea),
		AsOf:             auditTime(a.AsOf),
		Partial:          a.Partial,
		UnavailableUnits: a.UnavailableUnits,
	}
	for _, s := range a.Series {
		resp.Series = append(resp.Series, seriesPointResponse{Bucket: s.Bucket, Category: s.Category, Count: s.Count})
	}
	return c.JSON(resp)
}

func named(in []repository.NamedCount) []namedCountResponse {
	out := make([]namedCountResponse, 0, len(in))
	for _, n := range in {
		out = append(out, namedCountResponse{Name: n.Name, Count: n.Count})
	}
	return out
}

func parseAnalyticsFilter(c fiber.Ctx) (repository.AnalyticsFilter, error) {
	now := time.Now().UTC()
	f := repository.AnalyticsFilter{From: now.AddDate(0, 0, -30), To: now, Bucket: "day"}

	for key, target := range map[string]*time.Time{"from": &f.From, "to": &f.To} {
		if v := c.Query(key); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return f, fmt.Errorf("%s must be RFC 3339, for example 2026-11-01T00:00:00Z", key)
			}
			*target = t.UTC()
		}
	}
	if v := c.Query("bucket"); v != "" {
		f.Bucket = v
	}

	step, ok := repository.AnalyticsBuckets[f.Bucket]
	switch {
	case !ok:
		return f, errors.New("bucket must be hour, day, month or year")
	case !f.From.Before(f.To):
		return f, errors.New("from must be before to")
	case f.To.Sub(f.From)/step > repository.MaxAnalyticsBuckets:
		return f, fmt.Errorf("range has more than %d %s buckets, pick a larger bucket", repository.MaxAnalyticsBuckets, f.Bucket)
	}
	return f, nil
}
