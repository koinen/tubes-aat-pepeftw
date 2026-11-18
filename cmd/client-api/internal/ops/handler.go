// Package ops serves BNPB Internal Ops: the failed (unprocessed) messages and their replay. Every
// route proxies to the aggregator's ops API with OPS_TOKEN. Only metadata passes through: no
// payload reaches the response, the logs or the trace attributes.
package ops

import (
	"log/slog"
	"net/url"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/common"
)

const (
	// ScopeReadUnprocessed lets a client list failed messages. Only bnpb-ops holds it.
	ScopeReadUnprocessed = "hazard:read:unprocessed"
	// ScopeReplayUnprocessed lets a client start and follow replay jobs. Only bnpb-ops holds it.
	ScopeReplayUnprocessed = "hazard:replay:unprocessed"
)

type Handler struct {
	agg *aggregator.Client
	log *slog.Logger
}

func NewHandler(agg *aggregator.Client, log *slog.Logger) *Handler {
	return &Handler{agg: agg, log: log}
}

// Configured stops an ops request when client-api has no OPS_TOKEN. It runs after the scope check.
func (h *Handler) Configured(c fiber.Ctx) error {
	if !h.agg.OpsConfigured() {
		return common.WriteError(c, fiber.StatusServiceUnavailable, common.CodeUpstreamUnavailable, "ops access not configured")
	}
	return c.Next()
}

// ListUnprocessed returns retrying and parked messages: GET /unprocessed?source&since&limit&cursor.
func (h *Handler) ListUnprocessed(c fiber.Ctx) error {
	path := "/internal/unprocessed" + forwardQuery(c, "source", "since", "limit", "cursor")
	result, status, err := h.agg.Ops(c.Context(), fiber.MethodGet, path, common.CorrelationIDFrom(c), "")
	return h.respond(c, result, status, err)
}

// RequestReplay starts a replay job, or returns the source's active one: POST /unprocessed/replay?source.
// The JWT subject goes to the aggregator as the requester.
func (h *Handler) RequestReplay(c fiber.Ctx) error {
	path := "/internal/unprocessed/replay" + forwardQuery(c, "source")
	result, status, err := h.agg.Ops(c.Context(), fiber.MethodPost, path, common.CorrelationIDFrom(c), common.GetSub(c))
	return h.respond(c, result, status, err)
}

// GetReplayJob returns a replay job's progress: GET /unprocessed/replay/{job_id}.
func (h *Handler) GetReplayJob(c fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("job_id"))
	if err != nil {
		return common.WriteError(c, fiber.StatusBadRequest, common.CodeValidation, "job_id must be a UUID")
	}
	path := "/internal/unprocessed/replay/" + jobID.String()
	result, status, err := h.agg.Ops(c.Context(), fiber.MethodGet, path, common.CorrelationIDFrom(c), "")
	return h.respond(c, result, status, err)
}

// respond passes a success through and turns any other aggregator answer into client-api's error
// body. A rejected OPS_TOKEN means the two services disagree on the secret, so the caller sees an
// unavailable upstream and the log says why.
func (h *Handler) respond(c fiber.Ctx, result map[string]any, status int, err error) error {
	switch {
	case err == nil && (status == fiber.StatusOK || status == fiber.StatusAccepted):
		return c.Status(status).JSON(result)
	case err == nil && status == fiber.StatusBadRequest:
		return common.WriteError(c, status, common.CodeValidation, upstreamMessage(result, "bad request"))
	case err == nil && status == fiber.StatusNotFound:
		return common.WriteError(c, status, common.CodeNotFound, upstreamMessage(result, "not found"))
	case err == nil && status == fiber.StatusUnauthorized:
		h.log.Error("ops_token_rejected", "correlation_id", common.CorrelationIDFrom(c), "path", c.Path())
		return common.WriteError(c, fiber.StatusServiceUnavailable, common.CodeUpstreamUnavailable, "upstream aggregator unavailable")
	}
	c.Set(fiber.HeaderRetryAfter, "5")
	return common.WriteError(c, fiber.StatusServiceUnavailable, common.CodeUpstreamUnavailable, "upstream aggregator unavailable")
}

// upstreamMessage is the aggregator's error message, or fallback if it has none.
func upstreamMessage(result map[string]any, fallback string) string {
	e, _ := result["error"].(map[string]any)
	if m, ok := e["message"].(string); ok && m != "" {
		return m
	}
	return fallback
}

// forwardQuery keeps only the named query parameters.
func forwardQuery(c fiber.Ctx, names ...string) string {
	q := url.Values{}
	for _, n := range names {
		if v := c.Query(n); v != "" {
			q.Set(n, v)
		}
	}
	if encoded := q.Encode(); encoded != "" {
		return "?" + encoded
	}
	return ""
}
