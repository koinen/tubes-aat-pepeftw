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

func parseHazardFilter(c fiber.Ctx) (repository.HazardFilter, error) {
	var f repository.HazardFilter

	if v := c.Query("type"); v != "" {
		t := model.HazardType(v)
		if t != model.HazardSeismic && t != model.HazardVolcanic {
			return f, errors.New("type must be SEISMIC or VOLCANIC")
		}
		f.Type = &t
	}

	if v := c.Query("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, errors.New("since must be RFC 3339, for example 2026-10-01T07:00:00Z")
		}
		f.Since = &t
	}

	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > repository.MaxLimit {
			return f, fmt.Errorf("limit must be between 1 and %d", repository.MaxLimit)
		}
		f.Limit = n
	}

	return f, nil
}
