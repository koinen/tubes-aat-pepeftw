package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	retryAttempts     = 5
	retryBaseDelay    = 20 * time.Millisecond
	serializationCode = "40001"
)

func withRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if err = fn(); !isSerializationFailure(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryBaseDelay << attempt):
		}
	}
	return err
}

func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == serializationCode
}
