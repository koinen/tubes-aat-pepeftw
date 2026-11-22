package httpapi

import (
	"errors"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type auditEventsResponse struct {
	Items            []model.HazardEvent `json:"items"`
	NextCursor       *string             `json:"next_cursor"`
	Partial          bool                `json:"partial"`
	UnavailableUnits []string            `json:"unavailable_units"`
}

type auditChecksumResponse struct {
	Source           model.Source `json:"source"`
	Count            int          `json:"count"`
	Checksum         string       `json:"checksum"`
	Algorithm        string       `json:"algorithm"`
	AsOf             string       `json:"as_of"`
	Partial          bool         `json:"partial"`
	UnavailableUnits []string     `json:"unavailable_units"`
}

type auditLocationResponse struct {
	HazardID  string                `json:"hazard_id"`
	Locations []repository.Location `json:"locations"`
	Partial   bool                  `json:"partial"`
}

type auditUnprocessedResponse struct {
	Items      []repository.UnprocessedMessage `json:"items"`
	NextCursor *string                         `json:"next_cursor"`
}

func (h handlers) auditEvents(c fiber.Ctx) error {
	f, err := parseAuditFilter(c, parseSource)
	if err != nil {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, err.Error())
	}
	page, err := h.audit.AuditEvents(c.Context(), f)
	if err != nil {
		return err
	}
	return c.JSON(auditEventsResponse{
		Items:            page.Items,
		NextCursor:       page.NextCursor,
		Partial:          page.Partial,
		UnavailableUnits: page.UnavailableUnits,
	})
}

func (h handlers) auditChecksum(c fiber.Ctx) error {
	source, err := parseSource(c.Query("source"))
	if err != nil {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, err.Error())
	}
	if source == nil {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, "source wajib diisi")
	}
	sum, err := h.audit.AuditChecksum(c.Context(), *source)
	if err != nil {
		return err
	}
	return c.JSON(auditChecksumResponse{
		Source:           sum.Source,
		Count:            sum.Count,
		Checksum:         sum.Checksum,
		Algorithm:        auditChecksumAlgo,
		AsOf:             auditTime(sum.AsOf),
		Partial:          sum.Partial,
		UnavailableUnits: sum.UnavailableUnits,
	})
}

func (h handlers) auditLocation(c fiber.Ctx) error {
	id := c.Query("hazard_id")
	if id == "" {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, "hazard_id wajib diisi")
	}
	loc, err := h.audit.HazardLocation(c.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		return writeAuditError(c, fiber.StatusNotFound, auditNotFound, "hazard_id tidak ditemukan")
	}
	if err != nil {
		return err
	}
	return c.JSON(auditLocationResponse{HazardID: loc.HazardID, Locations: loc.Locations, Partial: loc.Partial})
}

func (h handlers) auditUnprocessed(c fiber.Ctx) error {
	f, err := parseAuditFilter(c, parseUnprocessedSource)
	if err != nil {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, err.Error())
	}
	page, err := h.audit.Unprocessed(c.Context(), f)
	if errors.Is(err, repository.ErrUnavailable) {
		c.Set(fiber.HeaderRetryAfter, "5")
		return writeAuditError(c, fiber.StatusServiceUnavailable, auditUnavailable, "penyimpanan sedang tidak terjangkau")
	}
	if err != nil {
		return err
	}
	return c.JSON(auditUnprocessedResponse{Items: page.Items, NextCursor: page.NextCursor})
}
