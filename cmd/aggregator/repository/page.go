package repository

import "time"

type Page[T any] struct {
	Items            []T
	NextCursor       *string
	Partial          bool
	UnavailableUnits []string
	AsOf             time.Time
}
