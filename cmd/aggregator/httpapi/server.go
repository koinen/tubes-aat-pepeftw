package httpapi

import (
	"context"
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type Deps struct {
	Hazards      repository.HazardReader
	Statuses     repository.SourceStatusRepository
	Audit        repository.AuditReader
	Analytics    repository.AnalyticsReader
	PostgresPing func(context.Context) error
	KafkaPing    func(context.Context) error
	InternalKey  string
	AuditToken   string
	Log          *slog.Logger
}

type handlers struct {
	hazards       repository.HazardReader
	statuses      repository.SourceStatusRepository
	audit         repository.AuditReader
	analyticsRepo repository.AnalyticsReader
}

func New(d Deps) *fiber.App {
	onError := errorHandler(d.Log)
	app := fiber.New(fiber.Config{ErrorHandler: onError})
	h := handlers{hazards: d.Hazards, statuses: d.Statuses, audit: d.Audit, analyticsRepo: d.Analytics}

	app.Use(correlationID, requestLogger(d.Log, onError), recoverer.New())
	app.Get("/health", health(d.PostgresPing, d.KafkaPing))

	audit := app.Group(auditPrefix, requireAuditToken(d.AuditToken))
	audit.Get("/events", h.auditEvents)
	audit.Get("/checksum", h.auditChecksum)
	audit.Get("/location", h.auditLocation)
	audit.Get("/unprocessed", h.auditUnprocessed)

	internalKey := requireInternalKey(d.InternalKey)
	app.Get("/internal/hazards", internalKey, h.listHazards)
	app.Get("/internal/hazards/:hazard_id", internalKey, h.getHazard)
	app.Get("/internal/sources/status", internalKey, h.sourcesStatus)
	app.Get("/internal/analytics", internalKey, h.analytics)

	return app
}

func errorHandler(log *slog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		var fe *fiber.Error
		if errors.As(err, &fe) && fe.Code < fiber.StatusInternalServerError {
			if isAuditPath(c) {
				code := auditBadRequest
				if fe.Code == fiber.StatusNotFound {
					code = auditNotFound
				}
				return writeAuditError(c, fe.Code, code, fe.Message)
			}
			code := codeValidation
			if fe.Code == fiber.StatusNotFound {
				code = codeNotFound
			}
			return writeError(c, fe.Code, code, fe.Message)
		}
		log.Error("unhandled_error", "correlation_id", correlationIDFrom(c), "error", err.Error())
		if isAuditPath(c) {
			return writeAuditError(c, fiber.StatusInternalServerError, auditInternal, "internal error")
		}
		return writeError(c, fiber.StatusInternalServerError, codeInternal, "internal error")
	}
}
