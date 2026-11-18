package poller

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type Deps struct {
	Hazards     repository.HazardWriter
	Unprocessed repository.UnprocessedWriter
	Cursors     repository.CursorRepository
	Statuses    repository.SourceStatusRepository
	Volcanoes   repository.VolcanoRefRepository
	Log         *slog.Logger
	Now         func() time.Time

	// Used by the Sensor Pantai poller.
	Existing  repository.HazardGetter
	Positions repository.StreamCursorRepository
}

type Poller struct {
	runners []*runner
}

func New(cfg Config, d Deps) *Poller {
	if d.Now == nil {
		d.Now = time.Now
	}

	bmkgFetch := newFetcher(model.SourceBMKG, cfg.BMKGURL, cfg.BMKGTimeout,
		func(r *http.Request) { r.Header.Set("X-BMKG-Key", cfg.BMKGAPIKey) }, d.Log, d.Now)
	pvmbgFetch := newFetcher(model.SourcePVMBG, cfg.PVMBGURL, cfg.PVMBGTimeout,
		func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+cfg.PVMBGToken) }, d.Log, d.Now)

	runners := []*runner{
		{
			source: model.SourceBMKG, interval: cfg.PollInterval,
			breaker:  NewBreaker(cfg.BreakerFailures, cfg.BreakerOpenFor),
			statuses: d.Statuses, cycle: newBMKGPoller(bmkgFetch, d, cfg).cycle, log: d.Log, now: d.Now,
		},
		{
			source: model.SourcePVMBG, interval: cfg.PollInterval,
			breaker:  NewBreaker(cfg.BreakerFailures, cfg.BreakerOpenFor),
			statuses: d.Statuses, cycle: newPVMBGPoller(pvmbgFetch, d, cfg).cycle, log: d.Log, now: d.Now,
		},
	}
	if cfg.SensorPantaiURL != "" {
		sensorFetch := newFetcher(model.SourceSensorPantai, cfg.SensorPantaiURL, cfg.SensorPantaiTimeout,
			func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+cfg.SensorPantaiToken) }, d.Log, d.Now)
		runners = append(runners, &runner{
			source: model.SourceSensorPantai, interval: cfg.SensorPantaiPollInterval,
			// C10 replaces this breaker with the sensor's own values.
			breaker:  NewBreaker(cfg.BreakerFailures, cfg.BreakerOpenFor),
			statuses: d.Statuses, cycle: newSensorPantaiPoller(sensorFetch, d).cycle, log: d.Log, now: d.Now,
		})
	}
	return &Poller{runners: runners}
}

func (p *Poller) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, r := range p.runners {
		wg.Add(1)
		go func(r *runner) {
			defer wg.Done()
			r.run(ctx)
		}(r)
	}
	wg.Wait()
}
