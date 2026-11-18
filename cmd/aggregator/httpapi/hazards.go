package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	sourcesTimeout   = time.Second
	sourceStatusUnit = "source-status"
)

type hazardListResponse struct {
	Data             []model.HazardEvent          `json:"data"`
	Sources          map[model.Source]sourceEntry `json:"sources"`
	NextCursor       *string                      `json:"next_cursor"`
	Partial          bool                         `json:"partial"`
	UnavailableUnits []string                     `json:"unavailable_units"`
}

func (h handlers) listHazards(c fiber.Ctx) error {
	filter, err := parseHazardFilter(c)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, codeValidation, err.Error())
	}

	page, err := h.hazards.ListHazards(c.Context(), filter)
	if err != nil {
		return err
	}

	unavailable := page.UnavailableUnits
	sourcesCtx, cancel := context.WithTimeout(c.Context(), sourcesTimeout)
	sources, err := h.sourcesBlock(sourcesCtx)
	cancel()
	if err != nil {
		sources = map[model.Source]sourceEntry{}
		unavailable = append(unavailable, sourceStatusUnit)
	}

	return c.JSON(hazardListResponse{
		Data:             page.Items,
		Sources:          sources,
		NextCursor:       page.NextCursor,
		Partial:          len(unavailable) > 0,
		UnavailableUnits: unavailable,
	})
}

func (h handlers) getHazard(c fiber.Ctx) error {
	id := c.Params("hazard_id")
	if _, err := uuid.Parse(id); err != nil {
		return writeError(c, fiber.StatusBadRequest, codeValidation, "hazard_id must be a UUID")
	}

	hazard, err := h.hazards.GetHazard(c.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		return writeError(c, fiber.StatusNotFound, codeNotFound, "hazard not found")
	}
	if err != nil {
		return err
	}
	return writeData(c, hazard)
}
