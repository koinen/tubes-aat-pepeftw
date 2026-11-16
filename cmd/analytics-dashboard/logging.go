package main

import (
	"log/slog"
	"os"
	"time"
)

func newLogger(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.String("ts", a.Value.Time().UTC().Truncate(time.Second).Format(time.RFC3339))
			}
			return a
		},
	})
	return slog.New(h).With("service", service)
}
