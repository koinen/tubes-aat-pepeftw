package httpapi

import (
	"errors"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type hazardListResponse struct {
	Data    []model.HazardEvent          `json:"data"`
	Sources map[model.Source]sourceEntry `json:"sources"`
}

func (h handlers) listHazards(c fiber.Ctx) error {
	filter, err := parseHazardFilter(c)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, codeValidation, err.Error())
	}

	hazards, err := h.hazards.ListHazards(c.Context(), filter)
	if err != nil {
		return err
	}
	sources, err := sourcesBlock(c.Context(), h.statuses)
	if err != nil {
		return err
	}
	return c.JSON(hazardListResponse{Data: hazards, Sources: sources})
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
