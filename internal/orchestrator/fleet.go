package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/hstern/fj-bellows/internal/control/events"
)

// Fleet coordinates independent tier orchestrators and presents one aggregate
// control-plane surface. Provisioning decisions remain inside each tier's
// single-writer reconcile loop.
type Fleet struct {
	tiers  map[string]*Orchestrator
	events *events.Bus
}

// NewFleet builds a fleet from fully wired tier orchestrators.
func NewFleet(tiers map[string]*Orchestrator) (*Fleet, error) {
	if len(tiers) == 0 {
		return nil, errors.New("orchestrator: fleet requires at least one tier")
	}
	cp := make(map[string]*Orchestrator, len(tiers))
	for name, tier := range tiers {
		if name == "" || tier == nil {
			return nil, errors.New("orchestrator: fleet tier names and values must be non-empty")
		}
		cp[name] = tier
	}
	return &Fleet{tiers: cp, events: events.New()}, nil
}

// TierNames returns stable lexical tier ordering.
func (f *Fleet) TierNames() []string {
	out := make([]string, 0, len(f.tiers))
	for name := range f.tiers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Run bridges per-tier events and runs all tier loops until cancellation.
func (f *Fleet) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, len(f.tiers))
	for _, name := range f.TierNames() {
		o := f.tiers[name]
		ch, unsubscribe := o.Subscribe()
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer unsubscribe()
			for {
				select {
				case <-runCtx.Done():
					return
				case ev, ok := <-ch:
					if !ok {
						return
					}
					f.events.Publish(ev)
				}
			}
		}()
		go func() {
			defer wg.Done()
			if err := o.Run(runCtx); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case err := <-errCh:
		<-done
		return err
	case <-ctx.Done():
		cancel()
		<-done
		return nil
	case <-done:
		return nil
	}
}

// Subscribe returns the aggregate stream of state transitions from all tiers.
func (f *Fleet) Subscribe() (<-chan events.Event, func()) { return f.events.Subscribe() }

// PublishEvent adds a fleet-adjacent subsystem event (such as an automatic
// routing decision) to the aggregate operator stream.
func (f *Fleet) PublishEvent(event events.Event) { f.events.Publish(event) }

// PoolSnapshot returns every tier's workers in deterministic order.
func (f *Fleet) PoolSnapshot() []WorkerView {
	var out []WorkerView
	for _, name := range f.TierNames() {
		out = append(out, f.tiers[name].PoolSnapshot()...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tier == out[j].Tier {
			return out[i].InstanceID < out[j].InstanceID
		}
		return out[i].Tier < out[j].Tier
	})
	return out
}

// RoutingSnapshots returns candidate capacity keyed by tier name.
func (f *Fleet) RoutingSnapshots(ctx context.Context) map[string]RoutingTierSnapshot {
	out := make(map[string]RoutingTierSnapshot, len(f.tiers))
	for name, tier := range f.tiers {
		out[name] = tier.RoutingSnapshot(ctx)
	}
	return out
}

// Health aggregates the least-recent successful signal and requires every
// tier to be healthy.
func (f *Fleet) Health(ctx context.Context) HealthStatus {
	out := HealthStatus{Healthy: true}
	first := true
	for _, name := range f.TierNames() {
		h := f.tiers[name].Health(ctx)
		out.Healthy = out.Healthy && h.Healthy
		out.Paused = out.Paused || h.Paused
		if first || h.LastTickAt.Before(out.LastTickAt) {
			out.LastTickAt = h.LastTickAt
		}
		if first || h.LastProviderListAt.Before(out.LastProviderListAt) {
			out.LastProviderListAt = h.LastProviderListAt
		}
		if first || h.LastForgejoPollAt.Before(out.LastForgejoPollAt) {
			out.LastForgejoPollAt = h.LastForgejoPollAt
		}
		first = false
	}
	return out
}

// Kick reconciles every tier concurrently and sums the result.
func (f *Fleet) Kick(ctx context.Context) (ReconcileResult, error) {
	type result struct {
		r   ReconcileResult
		err error
	}
	ch := make(chan result, len(f.tiers))
	for _, o := range f.tiers {
		go func() { r, err := o.Kick(ctx); ch <- result{r: r, err: err} }()
	}
	var out ReconcileResult
	for range f.tiers {
		r := <-ch
		if r.err != nil {
			return out, r.err
		}
		out.Provisioned += r.r.Provisioned
		out.Dispatched += r.r.Dispatched
		out.Reaped += r.r.Reaped
		out.Adopted += r.r.Adopted
		out.Dropped += r.r.Dropped
		out.Errors = append(out.Errors, r.r.Errors...)
	}
	return out, nil
}

func (f *Fleet) tier(name string) (*Orchestrator, error) {
	if name != "" {
		o, ok := f.tiers[name]
		if !ok {
			return nil, fmt.Errorf("unknown tier %q", name)
		}
		return o, nil
	}
	if len(f.tiers) != 1 {
		return nil, errors.New("tier selector is required for a multi-tier fleet")
	}
	for _, o := range f.tiers {
		return o, nil
	}
	panic("unreachable")
}

// ForceProvision provisions in a single-tier fleet.
func (f *Fleet) ForceProvision(ctx context.Context) (string, error) {
	return f.ForceProvisionIn(ctx, "")
}

// ForceProvisionIn provisions one worker in the selected tier.
func (f *Fleet) ForceProvisionIn(ctx context.Context, tier string) (string, error) {
	o, err := f.tier(tier)
	if err != nil {
		return "", err
	}
	return o.ForceProvision(ctx)
}

// ForceReap destroys an unambiguous worker selected across the fleet.
func (f *Fleet) ForceReap(ctx context.Context, instanceID string) error {
	return f.ForceReapIn(ctx, "", instanceID)
}

// ForceReapIn destroys a worker in the selected tier.
func (f *Fleet) ForceReapIn(ctx context.Context, tier, instanceID string) error {
	if tier != "" {
		o, err := f.tier(tier)
		if err != nil {
			return err
		}
		return o.ForceReap(ctx, instanceID)
	}
	var matches []*Orchestrator
	for _, o := range f.tiers {
		for _, worker := range o.PoolSnapshot() {
			if worker.InstanceID == instanceID {
				matches = append(matches, o)
				break
			}
		}
	}
	if len(matches) != 1 {
		return fmt.Errorf("instance %q matched %d tiers; provide a tier selector", instanceID, len(matches))
	}
	return matches[0].ForceReap(ctx, instanceID)
}

// Pause suppresses automatic reconcile ticks in every tier.
func (f *Fleet) Pause(ctx context.Context) {
	for _, o := range f.tiers {
		o.Pause(ctx)
	}
}

// Resume restores automatic reconcile ticks in every tier.
func (f *Fleet) Resume(ctx context.Context) {
	for _, o := range f.tiers {
		o.Resume(ctx)
	}
}

// ExecOnWorker executes a command on an unambiguous worker across the fleet.
func (f *Fleet) ExecOnWorker(ctx context.Context, instanceID, command string) (ExecResult, error) {
	return f.ExecOnWorkerIn(ctx, "", instanceID, command)
}

// ExecOnWorkerIn executes a command on a worker in the selected tier.
func (f *Fleet) ExecOnWorkerIn(ctx context.Context, tier, instanceID, command string) (ExecResult, error) {
	if tier != "" {
		o, err := f.tier(tier)
		if err != nil {
			return ExecResult{}, err
		}
		return o.ExecOnWorker(ctx, instanceID, command)
	}
	var match *Orchestrator
	for _, o := range f.tiers {
		for _, worker := range o.PoolSnapshot() {
			if worker.InstanceID != instanceID {
				continue
			}
			if match != nil {
				return ExecResult{}, fmt.Errorf("instance %q is ambiguous; provide a tier selector", instanceID)
			}
			match = o
		}
	}
	if match == nil {
		return ExecResult{}, fmt.Errorf("instance %q not in fleet", instanceID)
	}
	return match.ExecOnWorker(ctx, instanceID, command)
}

// ApplyHotConfig updates one tier without rebuilding its provider/dispatcher.
func (f *Fleet) ApplyHotConfig(tier string, cfg Config) ([]string, error) {
	o, ok := f.tiers[tier]
	if !ok {
		return nil, fmt.Errorf("unknown tier %q", tier)
	}
	return o.ApplyHotConfig(cfg)
}

// CurrentConfigs returns a snapshot of every tier's live runtime configuration.
func (f *Fleet) CurrentConfigs() map[string]Config {
	out := make(map[string]Config, len(f.tiers))
	for name, o := range f.tiers {
		out[name] = o.CurrentConfig()
	}
	return out
}
