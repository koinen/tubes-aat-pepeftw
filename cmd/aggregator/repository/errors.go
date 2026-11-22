package repository

import "errors"

var (
	ErrNotFound    = errors.New("not found")
	ErrUnavailable = errors.New("storage unavailable")
	// ErrRejected: the store refused a value in the record (SQLSTATE class 22 or 23), so retrying it will not help.
	ErrRejected = errors.New("storage rejected the value")
)
