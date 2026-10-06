package common

import "github.com/gofiber/fiber/v3"

const (
	codeValidation   = "VALIDATION_ERROR"
	codeUnauthorized = "UNAUTHORIZED"
	codeNotFound     = "NOT_FOUND"
	codeInternal     = "INTERNAL"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

func WriteError(c fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(errorBody{Error: errorDetail{
		Code:          code,
		Message:       message,
		CorrelationID: correlationIDFrom(c),
	}})
}
