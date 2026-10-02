package fixture

import (
	"context"
	"fmt"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

func Run(ctx context.Context, hazards repository.HazardWriter, statuses repository.SourceStatusRepository, now time.Time) error {
	for _, h := range fixtureHazards(now) {
		if _, err := hazards.SaveHazard(ctx, h); err != nil {
			return fmt.Errorf("seed hazard %s: %w", h.HazardID, err)
		}
	}
	for _, st := range fixtureStatuses(now) {
		if err := statuses.SaveSourceStatus(ctx, st); err != nil {
			return fmt.Errorf("seed status %s: %w", st.Source, err)
		}
	}
	return nil
}
