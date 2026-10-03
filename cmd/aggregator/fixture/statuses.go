package fixture

import (
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

func fixtureStatuses(now time.Time) []model.SourceStatus {
	pvmbgLastSuccess := now.Add(-5 * time.Minute)
	return []model.SourceStatus{
		{
			Source:        model.SourceBMKG,
			Status:        model.StatusOK,
			BreakerState:  model.BreakerClosed,
			LastSuccessAt: &now,
		},
		{
			Source:        model.SourcePVMBG,
			Status:        model.StatusStale,
			BreakerState:  model.BreakerOpen,
			LastSuccessAt: &pvmbgLastSuccess,
			StaleSince:    &pvmbgLastSuccess,
			LastError:     "timeout after 4s",
		},
	}
}
