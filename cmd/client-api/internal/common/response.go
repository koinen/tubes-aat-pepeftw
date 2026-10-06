package common

import "github.com/gofiber/fiber/v3"

const (
	CodeValidation          = "VALIDATION_ERROR"
	CodeUnauthorized        = "UNAUTHORIZED"
	CodeForbidden           = "FORBIDDEN"
	CodeForbiddenField      = "FORBIDDEN_FIELD"
	CodeNotFound            = "NOT_FOUND"
	CodeRateLimited         = "RATE_LIMITED"
	CodeUpstreamUnavailable = "UPSTREAM_UNAVAILABLE"
	CodeInternal            = "INTERNAL"
)

type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

func WriteError(c fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(ErrorBody{Error: ErrorDetail{
		Code:          code,
		Message:       message,
		CorrelationID: CorrelationIDFrom(c),
	}})
}
