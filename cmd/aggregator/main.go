package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/config"
	"tubes-aat-pepeftw/cmd/aggregator/fieldreport"
	"tubes-aat-pepeftw/cmd/aggregator/forwarded"
	"tubes-aat-pepeftw/cmd/aggregator/httpapi"
	"tubes-aat-pepeftw/cmd/aggregator/kafka"
	"tubes-aat-pepeftw/cmd/aggregator/leader"
	"tubes-aat-pepeftw/cmd/aggregator/logging"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/poller"
	"tubes-aat-pepeftw/cmd/aggregator/relay"
	"tubes-aat-pepeftw/cmd/aggregator/repository/postgres"
	"tubes-aat-pepeftw/cmd/aggregator/satellite"
	"tubes-aat-pepeftw/cmd/aggregator/unprocessed"

	"tubes-aat-pepeftw/internal/telemetry"
)

const (
	dbWaitTimeout  = 30 * time.Second
	dbPingTimeout  = 2 * time.Second
	relayBatchSize = 100
)

func main() {
	log := logging.New("aggregator")
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := runMigrate(log); err != nil {
			log.Error("migrate_failed", "error", err.Error())
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "load-catalog" {
		if err := runLoadCatalog(log, os.Args[2:]); err != nil {
			log.Error("load_catalog_failed", "error", err.Error())
			os.Exit(1)
		}
		return
	}
	if err := run(log); err != nil {
		log.Error("aggregator_stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pollerCfg, err := poller.LoadConfig()
	if err != nil {
		return err
	}

	leaderCfg, err := leader.LoadConfig()
	if err != nil {
		return err
	}

	satCfg, err := satellite.LoadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Init(ctx, "aggregator")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := waitForDB(ctx, pool, log); err != nil {
		return err
	}
	log.Info("database_connected")

	repo := postgres.New(pool).WithReadTimeouts(cfg.FastRead, cfg.ShardRead)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.HazardTopic)
	if err != nil {
		return err
	}
	defer producer.Close()

	relayOnLeader := pollerCfg.Enabled || satCfg.Enabled
	if !relayOnLeader {
		relayDone := make(chan struct{})
		go func() {
			defer close(relayDone)
			relay.New(repo, producer, log, cfg.RelayInterval, relayBatchSize).Run(ctx)
		}()
		defer func() {
			stop()
			<-relayDone
		}()
	}

	// Every pod consumes field reports, outside leader election; the writes are unfenced and idempotent.
	reports, err := kafka.NewConsumer(cfg.KafkaBrokers, fieldreport.GroupID, cfg.FieldReportsTopic,
		fieldreport.NewHandler(repo, log).HandleRecord, log)
	if err != nil {
		return err
	}
	reportsDone := make(chan struct{})
	go func() {
		defer close(reportsDone)
		reports.Run(ctx)
	}()
	defer func() {
		stop()
		<-reportsDone
	}()

	// Every pod also parks what our hazard-events consumers forwarded to topic unprocessed.
	forwards, err := kafka.NewConsumer(cfg.KafkaBrokers, forwarded.GroupID, cfg.UnprocessedTopic,
		forwarded.NewHandler(repo, log).Handle, log)
	if err != nil {
		return err
	}
	forwardsDone := make(chan struct{})
	go func() {
		defer close(forwardsDone)
		forwards.Run(ctx)
	}()
	defer func() {
		stop()
		<-forwardsDone
	}()

	// Only PVMBG can be replayed (see docs: replayable sources).
	replayRemappers := map[model.Source]unprocessed.Remapper{
		model.SourcePVMBG: unprocessed.PVMBGRemapper(repo, pollerCfg.PVMBGAdaptations),
	}
	replayRules := make(map[model.Source]string, len(replayRemappers))
	for source, rm := range replayRemappers {
		replayRules[source] = rm.Rules
	}

	if pollerCfg.Enabled || satCfg.Enabled {
		leaderDone := make(chan struct{})
		go func() {
			defer close(leaderDone)
			err := leader.Run(ctx, leaderCfg, log, func(leadCtx context.Context) {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					relay.New(repo, producer, log, cfg.RelayInterval, relayBatchSize).Run(leadCtx)
				}()
				if pollerCfg.Enabled {
					wg.Add(1)
					go func() {
						defer wg.Done()
						poller.New(pollerCfg, poller.Deps{
							Hazards:     repo,
							Unprocessed: repo,
							Cursors:     repo,
							Statuses:    repo,
							Volcanoes:   repo,
							Existing:    repo,
							Positions:   repo,
							Log:         log,
						}).Run(leadCtx)
					}()
					wg.Add(1)
					go func() {
						defer wg.Done()
						unprocessed.NewWorker(repo, log).Run(leadCtx)
					}()
					wg.Add(1)
					go func() {
						defer wg.Done()
						unprocessed.NewReplayer(repo, replayRemappers, log, time.Now).Run(leadCtx)
					}()
				}
				if satCfg.Enabled {
					wg.Add(1)
					go func() {
						defer wg.Done()
						satellite.New(satCfg, satellite.Deps{
							Park: repo.SaveUnprocessed,
							LastOffset: func(c context.Context) (int64, error) {
								s, err := repo.SatelliteState(c)
								return s.LastOffset, err
							},
							Apply: func(c context.Context, ev []satellite.Event, lateness time.Duration, at time.Time) (satellite.BatchStats, error) {
								r, err := repo.ApplySatelliteBatch(c, ev, lateness, at)
								return satellite.BatchStats{
									LastOffset: r.LastOffset, Detections: r.Detections, Duplicates: r.Duplicates,
									Late: r.Late, Updates: r.Updates, Retracts: r.Retracts,
									Orphans: r.Orphans, Gaps: r.Gaps,
								}, err
							},
							Log: log,
						}).Run(leadCtx)
					}()
					wg.Add(1)
					go func() {
						defer wg.Done()
						rec := satellite.NewRecognizer(wildfireStore{repo}, log, 5*time.Second).WithObserver(satellite.NewPromMetrics())
						if err := rec.Run(leadCtx); err != nil {
							log.Error("wildfire_recognizer_stopped", "error", err.Error())
						}
					}()
					wg.Add(1)
					go func() {
						defer wg.Done()
						satellite.NewMonitor(satCfg, wildfireStore{repo}, satellite.NewPromMetrics(), log).Run(leadCtx)
					}()
				}
				wg.Wait()
			})
			if err != nil {
				log.Error("leader_election_failed", "error", err.Error())
				stop()
			}
		}()
		defer func() {
			stop()
			<-leaderDone
		}()
		log.Info("leader_workers_ready", "poller", pollerCfg.Enabled, "satellite", satCfg.Enabled)
	}

	app := httpapi.New(httpapi.Deps{
		Hazards:             repo,
		Statuses:            repo,
		SensorPantaiEnabled: pollerCfg.Enabled && pollerCfg.SensorPantaiURL != "",
		Audit:               repo,
		Analytics:           repo,
		PostgresPing:        pool.Ping,
		KafkaPing:           producer.Ping,
		InternalKey:         cfg.InternalAPIKey,
		AuditToken:          cfg.AuditToken,
		Replay:              repo,
		ReplayRules:         replayRules,
		OpsToken:            cfg.OpsToken,
		Log:                 log,
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(shutdownCtx); err != nil {
			log.Error("shutdown_failed", "error", err.Error())
		}
	}()

	if cfg.AuditToken == "" {
		log.Warn("audit_token_missing", "effect", "audit api rejects every request")
	}
	if cfg.OpsToken == "" {
		log.Warn("ops_token_missing", "effect", "replay api rejects every request")
	}
	log.Info("aggregator_started", "port", cfg.Port)
	return app.Listen(":"+cfg.Port, fiber.ListenConfig{DisableStartupMessage: true})
}

func waitForDB(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	deadline := time.Now().Add(dbWaitTimeout)
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, dbPingTimeout)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("database unreachable after %s: %w", dbWaitTimeout, err)
		}
		log.Warn("database_unavailable", "attempt", attempt, "error", err.Error())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
