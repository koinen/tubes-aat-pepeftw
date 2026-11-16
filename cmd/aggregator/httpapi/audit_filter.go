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

func parseAuditFilter(c fiber.Ctx) (repository.AuditFilter, error) {
	var f repository.AuditFilter

	source, err := parseSource(c.Query("source"))
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
