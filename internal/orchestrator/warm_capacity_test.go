package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hstern/fj-bellows/internal/forgejo"
	omock "github.com/hstern/fj-bellows/internal/orchestrator/mock"
	"github.com/hstern/fj-bellows/internal/provider"
)

type optimizationQueueJobSource struct {
	*omock.JobSource
	held map[string]bool
}

func (s *optimizationQueueJobSource) HoldForRoutingOptimization(handle string) bool {
	return s.held[handle]
}

func TestReconcileProvisionsConfiguredWarmReserveWithoutJobs(t *testing.T) {
	prov := trackingProvider()
	cfg := baseConfig()
	cfg.MaxScale = 3
	cfg.WarmInstances = 2
	o := New(cfg, prov, &omock.JobSource{}, &omock.Dispatcher{}, nil)

	result := o.Reconcile(t.Context())
	if result.Provisioned != 2 {
		t.Fatalf("first reconcile provisioned = %d, want 2", result.Provisioned)
	}
	waitFor(t, "two warm workers become idle", func() bool {
		return len(o.pool.ByState(StateIdle)) == 2
	})

	result = o.Reconcile(t.Context())
	if result.Provisioned != 0 {
		t.Fatalf("second reconcile provisioned = %d, want 0", result.Provisioned)
	}
	if got := prov.ProvisionCount(); got != 2 {
		t.Fatalf("ProvisionCount = %d, want 2", got)
	}
}

func TestReconcileKeepsWarmReserveWhileWorkerIsBusy(t *testing.T) {
	prov := trackingProvider(provider.Instance{
		ID: "busy-vm", IPv4: "192.0.2.30", CreatedAt: time.Now(),
	})
	jobs := &omock.JobSource{
		WaitingJobsFn: func(context.Context) ([]forgejo.WaitingJob, error) {
			return []forgejo.WaitingJob{{Handle: "job-1", Labels: []string{labelUbuntu}}}, nil
		},
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	disp := &omock.Dispatcher{
		RunJobFn: func(context.Context, string, string, forgejo.Registration, forgejo.WaitingJob) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		},
	}
	cfg := baseConfig()
	cfg.MaxScale = 2
	cfg.WarmInstances = 1
	o := New(cfg, prov, jobs, disp, nil)

	result := o.Reconcile(t.Context())
	if result.Dispatched != 1 || result.Provisioned != 1 {
		t.Fatalf("reconcile = {dispatched:%d provisioned:%d}, want {1 1}",
			result.Dispatched, result.Provisioned)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}
	waitFor(t, "one idle reserve alongside busy worker", func() bool {
		return len(o.pool.ByState(StateBusy)) == 1 && len(o.pool.ByState(StateIdle)) == 1
	})

	close(release)
	o.wg.Wait()
	if got := prov.ProvisionCount(); got != 1 {
		t.Fatalf("ProvisionCount = %d, want 1 reserve worker", got)
	}
}

func TestDispatchCreditsResettingWorkerAsFutureCapacity(t *testing.T) {
	prov := trackingProvider()
	cfg := baseConfig()
	cfg.MaxScale = 2
	o := New(cfg, prov, &omock.JobSource{}, &omock.Dispatcher{}, nil)
	o.pool.Put(&Node{
		InstanceID: "resetting-vm", State: StateResetting,
		CreatedAt: time.Now(), LastBusy: time.Now(),
	})

	_, provisioned := o.dispatchJobs(t.Context(), []forgejo.WaitingJob{{
		Handle: "waiting-job", Labels: []string{labelUbuntu},
	}})
	if provisioned != 0 {
		t.Fatalf("provisioned = %d, want 0 while reset is future capacity", provisioned)
	}
	if got := prov.ProvisionCount(); got != 0 {
		t.Fatalf("ProvisionCount = %d, want 0", got)
	}
}

func TestDispatchOnlySuppressesProvisionForOptimizationQueuedJobs(t *testing.T) {
	prov := trackingProvider()
	jobs := &optimizationQueueJobSource{
		JobSource: &omock.JobSource{},
		held:      map[string]bool{"paid-window-wait": true},
	}
	cfg := baseConfig()
	cfg.MaxScale = 3
	o := New(cfg, prov, jobs, &omock.Dispatcher{}, nil)

	_, provisioned := o.dispatchJobs(t.Context(), []forgejo.WaitingJob{
		{Handle: "paid-window-wait", Labels: []string{labelUbuntu}},
		{Handle: "overflow", Labels: []string{labelUbuntu}},
	})
	if provisioned != 1 {
		t.Fatalf("provisioned = %d, want one VM for overflow and none for held job", provisioned)
	}
	waitFor(t, "overflow capacity becomes idle", func() bool {
		return len(o.pool.ByState(StateIdle)) == 1
	})
	o.wg.Wait()
}
