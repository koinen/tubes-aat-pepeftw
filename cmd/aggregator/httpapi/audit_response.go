package httpapi

import (
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
)

const (
	auditPrefix          = "/internal/audit"
	auditBadRequest      = "bad_request"
	auditUnauthorized    = "unauthorized"
	auditNotFound        = "not_found"
	auditUnavailable     = "unavailable"
	auditInternal        = "internal"
	auditChecksumAlgo    = "sha256"
	auditTimestampFormat = time.RFC3339
)

type auditErrorBody struct {
	Error auditErrorDetail `json:"error"`
}

type auditErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeAuditError(c fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(auditErrorBody{Error: auditErrorDetail{Code: code, Message: message}})
}

// isAuditPath reports whether the request gets the audit error body (Lampiran A shape): the
// audit API and the replay API.
func isAuditPath(c fiber.Ctx) bool {
	return strings.HasPrefix(c.Path(), auditPrefix) || strings.HasPrefix(c.Path(), replayPrefix)
}

func auditTime(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(auditTimestampFormat)
}
