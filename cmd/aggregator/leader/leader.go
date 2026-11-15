package leader

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type Config struct {
	Enabled       bool
	LeaseName     string
	Namespace     string
	Identity      string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

func LoadConfig() (Config, error) {
	cfg := Config{
		Enabled:   os.Getenv("LEADER_ELECTION") == "true",
		LeaseName: getenv("LEASE_NAME", "aggregator-leader"),
		Namespace: os.Getenv("POD_NAMESPACE"),
		Identity:  os.Getenv("POD_NAME"),
	}

	var err error
	if cfg.LeaseDuration, err = duration("LEASE_DURATION", 15*time.Second); err != nil {
		return cfg, err
	}
	if cfg.RenewDeadline, err = duration("RENEW_DEADLINE", 10*time.Second); err != nil {
		return cfg, err
	}
	if cfg.RetryPeriod, err = duration("RETRY_PERIOD", 2*time.Second); err != nil {
		return cfg, err
	}

	if !cfg.Enabled {
		return cfg, nil
	}
	if cfg.Namespace == "" || cfg.Identity == "" {
		return cfg, errors.New("POD_NAMESPACE and POD_NAME are required when LEADER_ELECTION=true")
	}
	if !(cfg.LeaseDuration > cfg.RenewDeadline && cfg.RenewDeadline > cfg.RetryPeriod) {
		return cfg, errors.New("need LEASE_DURATION > RENEW_DEADLINE > RETRY_PERIOD")
	}
	return cfg, nil
}

func Run(ctx context.Context, cfg Config, log *slog.Logger, lead func(context.Context)) error {
	if !cfg.Enabled {
		log.Info("leader_election_disabled")
		lead(ctx)
		return nil
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		var wg sync.WaitGroup

		elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.LeaseLock{
				LeaseMeta:  metav1.ObjectMeta{Name: cfg.LeaseName, Namespace: cfg.Namespace},
				Client:     client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{Identity: cfg.Identity},
			},
			Name:            cfg.LeaseName,
			LeaseDuration:   cfg.LeaseDuration,
			RenewDeadline:   cfg.RenewDeadline,
			RetryPeriod:     cfg.RetryPeriod,
			ReleaseOnCancel: true,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(c context.Context) {
					wg.Add(1)
					defer wg.Done()
					log.Info("leader_acquired", "identity", cfg.Identity)
					lead(c)
				},
				OnStoppedLeading: func() {},
				OnNewLeader: func(id string) {
					log.Info("leader_observed", "leader", id, "self", id == cfg.Identity)
				},
			},
		})
		if err != nil {
			return err
		}

		elector.Run(ctx)
		wg.Wait()
		if ctx.Err() == nil {
			log.Warn("leader_lost", "identity", cfg.Identity)
		}
	}
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, errors.New(key + " must be a positive duration such as 15s")
	}
	return d, nil
}
