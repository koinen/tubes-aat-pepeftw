package health

import (
	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
)

type Handler struct {
	agg    *aggregator.Client
	jwksOK bool
}

func NewHandler(agg *aggregator.Client, jwksOK bool) *Handler {
	return &Handler{
		agg:    agg,
		jwksOK: jwksOK,
	}
}

type healthResponse struct {
	Status  string            `json:"status"`
	Service string            `json:"service"`
	Checks  map[string]string `json:"checks"`
}

func (h *Handler) HealthCheck(c fiber.Ctx) error {
	checks := map[string]string{
		"aggregator": "ok",
		"jwks":       "ok",
	}

	if err := h.agg.Ping(); err != nil {
		checks["aggregator"] = "unavailable"
	}
	if !h.jwksOK {
		checks["jwks"] = "unavailable"
	}

	status := "ok"
	for _, v := range checks {
		if v == "unavailable" {
			status = "unavailable"
			break
		}
	}

	httpStatus := fiber.StatusOK
	if status == "unavailable" {
		httpStatus = fiber.StatusServiceUnavailable
	}

	return c.Status(httpStatus).JSON(healthResponse{
		Status:  status,
		Service: "client-api",
		Checks:  checks,
	})
}
