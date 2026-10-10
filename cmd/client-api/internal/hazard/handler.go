package hazard

import (
	"net/url"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/common"
)

type Handler struct {
	agg *aggregator.Client
}

func NewHandler(agg *aggregator.Client) *Handler {
	return &Handler{agg: agg}
}

func (h *Handler) ListHazards(c fiber.Ctx) error {
	scope := common.GetScope(c)
	corrID := common.CorrelationIDFrom(c)

	fields, errCode, errMsg := h.parseFields(c, scope)
	if errCode != "" {
		status := fiber.StatusBadRequest
		if errCode == "FORBIDDEN_FIELD" {
			status = fiber.StatusForbidden
		}
		return common.WriteError(c, status, errCode, errMsg)
	}

	path := "/internal/hazards" + buildQuery(c)

	result, statusCode, err := h.agg.Get(path, corrID)
	if err != nil {
		c.Set("Retry-After", "5")
		return common.WriteError(c, fiber.StatusServiceUnavailable,
			common.CodeUpstreamUnavailable, "upstream aggregator unavailable")
	}
	if statusCode != fiber.StatusOK {
		return c.Status(statusCode).JSON(result)
	}

	if dataSlice, ok := result["data"].([]any); ok {
		projected := make([]any, 0, len(dataSlice))
		for _, item := range dataSlice {
			if hazardMap, ok := item.(map[string]any); ok {
				projected = append(projected, ProjectHazard(hazardMap, fields))
			}
		}
		result["data"] = projected
	}

	return c.Status(fiber.StatusOK).JSON(result)
}

func (h *Handler) GetHazard(c fiber.Ctx) error {
	scope := common.GetScope(c)
	corrID := common.CorrelationIDFrom(c)
	hazardID := c.Params("hazard_id")

	if _, err := uuid.Parse(hazardID); err != nil {
		return common.WriteError(c, fiber.StatusBadRequest, common.CodeValidation, "hazard_id must be a UUID")
	}

	fields, errCode, errMsg := h.parseFields(c, scope)
	if errCode != "" {
		status := fiber.StatusBadRequest
		if errCode == "FORBIDDEN_FIELD" {
			status = fiber.StatusForbidden
		}
		return common.WriteError(c, status, errCode, errMsg)
	}

	path := "/internal/hazards/" + url.PathEscape(hazardID)

	result, statusCode, err := h.agg.Get(path, corrID)
	if err != nil {
		c.Set("Retry-After", "5")
		return common.WriteError(c, fiber.StatusServiceUnavailable,
			common.CodeUpstreamUnavailable, "upstream aggregator unavailable")
	}
	if statusCode == fiber.StatusNotFound {
		return common.WriteError(c, fiber.StatusNotFound, common.CodeNotFound, "hazard not found")
	}
	if statusCode != fiber.StatusOK {
		return c.Status(statusCode).JSON(result)
	}

	if dataMap, ok := result["data"].(map[string]any); ok {
		result["data"] = ProjectHazard(dataMap, fields)
	}

	return c.Status(fiber.StatusOK).JSON(result)
}

func (h *Handler) parseFields(c fiber.Ctx, scope string) (fields []string, errCode, errMsg string) {
	raw := c.Query("fields")
	if raw == "" {
		return DefaultFields(scope), "", ""
	}

	parts := strings.Split(raw, ",")
	fields = make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			fields = append(fields, trimmed)
		}
	}

	if len(fields) == 0 {
		return DefaultFields(scope), "", ""
	}

	errCode, errMsg = ValidateFields(fields, scope)
	return fields, errCode, errMsg
}

func buildQuery(c fiber.Ctx) string {
	q := url.Values{}
	if v := c.Query("type"); v != "" {
		q.Set("type", v)
	}
	if v := c.Query("since"); v != "" {
		q.Set("since", v)
	}
	if v := c.Query("limit"); v != "" {
		q.Set("limit", v)
	}
	if encoded := q.Encode(); encoded != "" {
		return "?" + encoded
	}
	return ""
}
