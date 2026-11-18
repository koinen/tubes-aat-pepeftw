package httpapi

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

// parseAuditFilter reads the list filter; source is parsed by parseSource or, for the unprocessed
// lists, parseUnprocessedSource.
func parseAuditFilter(c fiber.Ctx, sourceOf func(string) (*model.Source, error)) (repository.AuditFilter, error) {
	var f repository.AuditFilter

	source, err := sourceOf(c.Query("source"))
	if err != nil {
		return f, err
	}
	f.Source = source

	if v := c.Query("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, errors.New("since harus ISO 8601, misalnya 2026-11-12T00:00:00Z")
		}
		f.Since = &t
	}

	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > repository.AuditMaxLimit {
			return f, fmt.Errorf("limit harus antara 1 dan %d", repository.AuditMaxLimit)
		}
		f.Limit = n
	}

	cursor, err := repository.DecodeKeyset(c.Query("cursor"))
	if err != nil {
		return f, errors.New("cursor tidak valid")
	}
	f.Cursor = cursor
	return f, nil
}

func parseSource(v string) (*model.Source, error) {
	if v == "" {
		return nil, nil
	}
	s := model.Source(v)
	if !s.Valid() {
		return nil, errors.New("source harus BMKG, PVMBG, SATELIT atau SENSOR_PANTAI")
	}
	return &s, nil
}

// parseUnprocessedSource also accepts the sources that are not hazard sources: unprocessed messages
// come from the field team (TIM_LAPANGAN) and from our own Kafka consumers (INTERNAL) too.
func parseUnprocessedSource(v string) (*model.Source, error) {
	if s := model.Source(v); s == model.SourceTimLapangan || s == model.SourceInternal {
		return &s, nil
	}
	source, err := parseSource(v)
	if err != nil {
		return nil, errors.New("source harus BMKG, PVMBG, SATELIT, SENSOR_PANTAI, TIM_LAPANGAN atau INTERNAL")
	}
	return source, nil
}
