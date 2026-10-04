package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"
)

const (
	healthTimeout     = 2 * time.Second
	heartbeatInterval = 15 * time.Second
)

//go:embed web/index.html
var indexHTML []byte

func newServer(ctx context.Context, recent *recentHazards, live *broadcaster, ping func(context.Context) error) *fiber.App {
	app := fiber.New()

	app.Get("/", func(c fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.Send(indexHTML)
	})

	app.Get("/api/hazards", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"data": recent.snapshot()})
	})

	app.Get("/health", func(c fiber.Ctx) error {
		pingCtx, cancel := context.WithTimeout(c.Context(), healthTimeout)
		defer cancel()
		status, httpStatus := "ok", fiber.StatusOK
		if err := ping(pingCtx); err != nil {
			status, httpStatus = "unavailable", fiber.StatusServiceUnavailable
		}
		return c.Status(httpStatus).JSON(fiber.Map{
			"status":  status,
			"service": "dashboard-updater",
			"checks":  fiber.Map{"kafka": status},
		})
	})

	app.Get("/events", func(c fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, "text/event-stream")
		c.Set(fiber.HeaderCacheControl, "no-cache")
		c.Set(fiber.HeaderConnection, "keep-alive")

		updates, unregister := live.register()
		snapshot := recent.snapshot()

		return c.SendStreamWriter(func(w *bufio.Writer) {
			defer unregister()

			for i := len(snapshot) - 1; i >= 0; i-- {
				if writeEvent(w, snapshot[i]) != nil {
					return
				}
			}
			if w.Flush() != nil {
				return
			}

			heartbeat := time.NewTicker(heartbeatInterval)
			defer heartbeat.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-updates:
					if !ok || writeEvent(w, v) != nil || w.Flush() != nil {
						return
					}
				case <-heartbeat.C:
					if _, err := w.WriteString(": ping\n\n"); err != nil || w.Flush() != nil {
						return
					}
				}
			}
		})
	})

	return app
}

func writeEvent(w *bufio.Writer, v hazardView) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: hazard\ndata: %s\n\n", data)
	return err
}
