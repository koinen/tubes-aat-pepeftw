package httpapi

import (
	"context"
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"

	"tubes-aat-pepeftw/internal/telemetry"
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
	// Replay and ReplayRules serve the replay API. ReplayRules holds, per replayable source, the
	// canonical adaptation rules of this pod; a source without an entry cannot be replayed.
	Replay      repository.ReplayJobs
	ReplayRules map[model.Source]string
	// OpsToken guards the replay API. Only client-api holds it; the audit token cannot call it.
	OpsToken string
	Log      *slog.Logger
}

type handlers struct {
	hazards       repository.HazardReader
	statuses      repository.SourceStatusRepository
	audit         repository.AuditReader
	analyticsRepo repository.AnalyticsReader
	replay        repository.ReplayJobs
	replayRules   map[model.Source]string
}

func New(d Deps) *fiber.App {
	onError := errorHandler(d.Log)
	app := fiber.New(fiber.Config{ErrorHandler: onError})
	h := handlers{hazards: d.Hazards, statuses: d.Statuses, audit: d.Audit, analyticsRepo: d.Analytics,
		replay: d.Replay, replayRules: d.ReplayRules}

	app.Use(telemetry.Middleware(), correlationID, requestLogger(d.Log, onError), recoverer.New())
	app.Get("/health", health(d.PostgresPing, d.KafkaPing))
	app.Get("/metrics", telemetry.MetricsHandler())

	audit := app.Group(auditPrefix, requireBearerToken(d.AuditToken))
	audit.Get("/events", h.auditEvents)
	audit.Get("/checksum", h.auditChecksum)
	audit.Get("/location", h.auditLocation)
	audit.Get("/unprocessed", h.auditUnprocessed)

	replay := app.Group(replayPrefix, requireBearerToken(d.OpsToken))
	replay.Post("", h.requestReplay)
	replay.Get("/:job_id", h.getReplayJob)

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
