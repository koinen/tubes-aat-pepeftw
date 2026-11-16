package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	retryAttempts     = 8
	retryBaseDelay    = 50 * time.Millisecond
	retryMaxDelay     = 2 * time.Second
	serializationCode = "40001"
	ambiguousCode     = "40003"
)

// Only for operations that are safe to repeat, because an ambiguous commit may already have landed.
func withIdempotentRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if err = fn(); !isRetryable(ctx, err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
	return err
}

func backoff(attempt int) time.Duration {
	d := retryBaseDelay << attempt
	if d > retryMaxDelay {
		return retryMaxDelay
	}
	return d
}

func isRetryable(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == serializationCode || pgErr.Code == ambiguousCode
	}
	var netErr net.Error
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.As(err, &netErr) || pgconn.SafeToRetry(err)
}
