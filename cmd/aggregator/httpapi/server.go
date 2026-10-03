package httpapi

import (
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type Deps struct {
	Hazards     repository.HazardReader
	Statuses    repository.SourceStatusRepository
	InternalKey string
	Log         *slog.Logger
}

type handlers struct {
	hazards  repository.HazardReader
	statuses repository.SourceStatusRepository
}

func New(d Deps) *fiber.App {
	onError := errorHandler(d.Log)
	app := fiber.New(fiber.Config{ErrorHandler: onError})
	h := handlers{hazards: d.Hazards, statuses: d.Statuses}

	app.Use(correlationID, requestLogger(d.Log, onError), recoverer.New())
	app.Get("/health", health)

	internal := app.Group("/internal", requireInternalKey(d.InternalKey))
	internal.Get("/hazards", h.listHazards)
	internal.Get("/hazards/:hazard_id", h.getHazard)
	internal.Get("/sources/status", h.sourcesStatus)

	return app
}

func errorHandler(log *slog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		var fe *fiber.Error
		if errors.As(err, &fe) && fe.Code < fiber.StatusInternalServerError {
			code := codeValidation
			if fe.Code == fiber.StatusNotFound {
				code = codeNotFound
			}
			return writeError(c, fe.Code, code, fe.Message)
		}
		log.Error("unhandled_error", "correlation_id", correlationIDFrom(c), "error", err.Error())
		return writeError(c, fiber.StatusInternalServerError, codeInternal, "internal error")
	}
}
