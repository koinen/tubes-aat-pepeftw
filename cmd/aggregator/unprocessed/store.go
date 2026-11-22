package unprocessed

import (
	"errors"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

// Recordable reports whether a store error belongs in the DLQ. Losing leadership, shutting down,
// and bugs in our own SQL are not message failures: the caller should return them instead.
func Recordable(err error) bool {
	return errors.Is(err, repository.ErrUnavailable) || errors.Is(err, repository.ErrRejected)
}
