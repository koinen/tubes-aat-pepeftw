package repository

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type VolcanoRefRepository interface {
	ListVolcanoRefs(ctx context.Context) ([]model.VolcanoRef, error)
}
