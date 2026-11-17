package main

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository/postgres"
	"tubes-aat-pepeftw/cmd/aggregator/satellite"
)

type wildfireStore struct{ repo *postgres.PostgresRepository }

func (w wildfireStore) ActiveDetections(ctx context.Context) ([]satellite.Detection, error) {
	return w.repo.ActiveDetections(ctx)
}

func (w wildfireStore) ListWildfires(ctx context.Context) ([]model.HazardEvent, error) {
	return w.repo.ListWildfires(ctx)
}

func (w wildfireStore) SaveWildfire(ctx context.Context, h model.HazardEvent) (string, error) {
	out, err := w.repo.SaveWildfire(ctx, h)
	return string(out), err
}

func (w wildfireStore) Version(ctx context.Context) (int64, error) {
	return w.repo.DetectionVersion(ctx)
}

func (w wildfireStore) Progress(ctx context.Context) (satellite.Progress, error) {
	return w.repo.Progress(ctx)
}
