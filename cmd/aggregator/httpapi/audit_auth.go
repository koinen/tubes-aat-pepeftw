package httpapi

import (
	"crypto/subtle"
	"strings"

	"github.com/gofiber/fiber/v3"
)

func requireAuditToken(token string) fiber.Handler {
	want := []byte(token)
	return func(c fiber.Ctx) error {
		got, ok := strings.CutPrefix(c.Get(fiber.HeaderAuthorization), "Bearer ")
		if !ok || len(want) == 0 || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), want) != 1 {
			return writeAuditError(c, fiber.StatusUnauthorized, auditUnauthorized, "token tidak valid")
		}
		return c.Next()
	}
}
