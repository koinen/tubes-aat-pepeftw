package httpapi

import (
	"errors"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	replayPrefix = "/internal/unprocessed/replay"
	// requestedByOpsToken is who asked for a replay until C6 forwards the JWT subject.
	requestedByOpsToken = "ops-token"
)

type replayAccepted struct {
	JobID  string             `json:"job_id"`
	Status model.ReplayStatus `json:"status"`
}

type replayJobResponse struct {
	JobID         string             `json:"job_id"`
	Source        model.Source       `json:"source"`
	Status        model.ReplayStatus `json:"status"`
	RequestedBy   string             `json:"requested_by"`
	Adaptations   string             `json:"adaptations"`
	Cutoff        string             `json:"cutoff"`
	Selected      int64              `json:"selected"`
	Recovered     int64              `json:"recovered"`
	StillParked   int64              `json:"still_parked"`
	HandedToRetry int64              `json:"handed_to_retry"`
	NotReplayable int64              `json:"not_replayable"`
	Error         *string            `json:"error"`
	CreatedAt     string             `json:"created_at"`
	StartedAt     *string            `json:"started_at"`
	FinishedAt    *string            `json:"finished_at"`
}

func (h handlers) requestReplay(c fiber.Ctx) error {
	source, err := parseSource(c.Query("source"))
	if err != nil {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, err.Error())
	}
	if source == nil {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, "source wajib diisi")
	}
	rules, ok := h.replayRules[*source]
	if !ok {
		return writeAuditError(c, fiber.StatusBadRequest, auditBadRequest, "source tidak mendukung replay")
	}
	job, err := h.replay.RequestReplay(c.Context(), *source, requestedByOpsToken, rules)
	if err != nil {
		return replayStoreError(c, err)
	}
	return c.Status(fiber.StatusAccepted).JSON(replayAccepted{JobID: job.JobID, Status: job.Status})
}

func (h handlers) getReplayJob(c fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("job_id"))
	if err != nil {
		return writeAuditError(c, fiber.StatusNotFound, auditNotFound, "job_id tidak ditemukan")
	}
	job, found, err := h.replay.ReplayJob(c.Context(), id.String())
	if err != nil {
		return replayStoreError(c, err)
	}
	if !found {
		return writeAuditError(c, fiber.StatusNotFound, auditNotFound, "job_id tidak ditemukan")
	}
	return c.JSON(replayJob(job))
}

func replayStoreError(c fiber.Ctx, err error) error {
	if errors.Is(err, repository.ErrUnavailable) {
		c.Set(fiber.HeaderRetryAfter, "5")
		return writeAuditError(c, fiber.StatusServiceUnavailable, auditUnavailable, "penyimpanan sedang tidak terjangkau")
	}
	return err
}

func replayJob(j model.ReplayJob) replayJobResponse {
	r := replayJobResponse{
		JobID: j.JobID, Source: j.Source, Status: j.Status, RequestedBy: j.RequestedBy, Adaptations: j.Adaptations,
		Cutoff: auditTime(j.Cutoff), Selected: j.Selected, Recovered: j.Recovered, StillParked: j.StillParked,
		HandedToRetry: j.HandedToRetry, NotReplayable: j.NotReplayable, CreatedAt: auditTime(j.CreatedAt),
		StartedAt: optionalTime(j.StartedAt), FinishedAt: optionalTime(j.FinishedAt),
	}
	if j.Error != "" {
		r.Error = &j.Error
	}
	return r
}

func optionalTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := auditTime(*t)
	return &s
}
