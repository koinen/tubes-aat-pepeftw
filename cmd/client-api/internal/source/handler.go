package source

import (
	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/common"
)

type Handler struct {
	agg *aggregator.Client
}

func NewHandler(agg *aggregator.Client) *Handler {
	return &Handler{agg: agg}
}

func (h *Handler) GetSourcesStatus(c fiber.Ctx) error {
	corrID := common.CorrelationIDFrom(c)

	result, statusCode, err := h.agg.Get("/internal/sources/status", corrID)
	if err != nil {
		c.Set("Retry-After", "5")
		return common.WriteError(c, fiber.StatusServiceUnavailable,
			common.CodeUpstreamUnavailable, "upstream aggregator unavailable")
	}
	return c.Status(statusCode).JSON(result)
}
