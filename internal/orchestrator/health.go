package orchestrator

import (
	"context"
	"time"
)

// HealthStatus is the orchestrator's view of its own readiness. The control
// plane's Health endpoint consumes it; the threshold for Healthy is
// 3 * PollInterval since the last successful tick / probe.
type HealthStatus struct {
	Healthy            bool
	LastTickAt         time.Time
	LastProviderListAt time.Time
	LastForgejoPollAt  time.Time
	// Paused reflects the reconciler's auto-tick suppression flag (FJB-27).
	// Operator-visible signal: when true, the freshness counters will go
	// stale because no real reconcile is firing, so Healthy will flip to
	// false on its own. The flag distinguishes "intentionally quiesced"
	// from "stuck upstream".
	Paused bool
}

// WorkerView is the per-node shape returned by PoolSnapshot. Stable,
// wire-friendly mirror of orchestrator.Node so the control plane can
// translate to protobuf without coupling the orchestrator to generated code.
//
// The billing-window fields (PaidHourEndAt, ReapEligibleAt, BillingModel)
// are populated from the orchestrator's TeardownPolicy via Timing(); see
// FJB-30.
type WorkerView struct {
	Tier         string
	ProviderName string
	Driver       string
	InstanceID   string
	State        string
	// IP is the public IPv4 (legacy dial address under ssh transport).
	IP string
	// VPCIP is the VPC-side IPv4 (dial address under cache-gateway
	// transport, FJB-54). Empty when no VPC is configured.
	VPCIP      string
	CreatedAt  time.Time
	LastBusy   time.Time
	CurrentJob string

	// PaidHourEndAt is the next paid-hour boundary for the worker — when
	// hourly-round-up billing models close out the next paid hour. Zero
	// for per-second models.
	PaidHourEndAt time.Time
	// ReapEligibleAt is when the worker first becomes eligible for
	// teardown under the current policy: LastBusy + IdleTimeout for
	// per-second, the next :55 mark for hourly.
	ReapEligibleAt time.Time
	// BillingModel is the policy's billing model string:
	// "per_second" | "hourly_round_up". Empty for the zero policy.
	BillingModel string
}

// RoutingTierSnapshot is the read-only capacity and billing view consumed by
// the fleet-level auto-router. It never reserves or mutates capacity.
type RoutingTierSnapshot struct {
	Tier              string
	ProviderName      string
	Driver            string
	InstanceType      string
	Labels            []string
	OneJobPerVM       bool
	ResetMode         string
	ResetMinRemaining time.Duration
	Healthy           bool
	MaxInstances      int
	ActiveWorkers     int
	PendingWorkers    int
	AvailableSlots    int
	IdleWorkers       []WorkerView
	Workers           []WorkerView
	Teardown          TeardownPolicy
}

// RoutingSnapshot returns a coherent-enough scoring snapshot. The target
// reconciler rechecks capacity before provisioning, so normal concurrent state
// movement can only delay a routed job, never exceed MaxScale.
func (o *Orchestrator) RoutingSnapshot(ctx context.Context) RoutingTierSnapshot {
	runtime := o.runtimeConfig()
	o.mu.Lock()
	pending, builders := o.pending, o.builders
	o.mu.Unlock()
	workers := o.PoolSnapshot()
	idle := make([]WorkerView, 0, len(workers))
	for _, worker := range workers {
		if worker.State == string(StateIdle) {
			idle = append(idle, worker)
		}
	}
	active := len(workers) + pending + builders
	return RoutingTierSnapshot{
		Tier: o.cfg.Tier, ProviderName: o.cfg.ProviderName, Driver: o.cfg.Driver,
		InstanceType: o.cfg.InstanceType, Labels: append([]string(nil), o.cfg.Labels...),
		OneJobPerVM: o.cfg.OneJobPerVM, ResetMode: o.cfg.ResetMode,
		ResetMinRemaining: runtime.ResetMinRemaining,
		Healthy:           o.Health(ctx).Healthy, MaxInstances: runtime.MaxScale,
		ActiveWorkers: len(workers), PendingWorkers: pending + builders,
		AvailableSlots: max(runtime.MaxScale-active, 0), IdleWorkers: idle,
		Workers:  workers,
		Teardown: runtime.Teardown,
	}
}

// PoolSnapshot returns a copy of every node currently in the pool.
// Equivalent of Pool.Snapshot but stringified for the wire (NodeState → string).
// Each view also carries the per-worker billing-window timing computed
// from the current TeardownPolicy.
func (o *Orchestrator) PoolSnapshot() []WorkerView {
	nodes := o.pool.Snapshot()
	now := o.now()
	runtime := o.runtimeConfig()
	out := make([]WorkerView, 0, len(nodes))
	for _, n := range nodes {
		t := runtime.Teardown.Timing(n, now)
		out = append(out, WorkerView{
			Tier:           o.cfg.Tier,
			ProviderName:   o.cfg.ProviderName,
			Driver:         o.cfg.Driver,
			InstanceID:     n.InstanceID,
			State:          string(n.State),
			IP:             n.IP,
			VPCIP:          n.VPCIP,
			CreatedAt:      n.CreatedAt,
			LastBusy:       n.LastBusy,
			CurrentJob:     n.CurrentJob,
			PaidHourEndAt:  t.PaidHourEndAt,
			ReapEligibleAt: t.ReapEligibleAt,
			BillingModel:   t.BillingModel,
		})
	}
	return out
}

// Health returns a snapshot of the freshness counters. The ctx is accepted to
// match a future interface where the answer might require an upstream probe;
// today it is unused.
func (o *Orchestrator) Health(_ context.Context) HealthStatus {
	o.mu.Lock()
	tick := o.lastTickAt
	prov := o.lastProviderListAt
	fj := o.lastForgejoPollAt
	o.mu.Unlock()

	threshold := 3 * o.runtimeConfig().PollInterval
	now := o.now()
	healthy := !tick.IsZero() &&
		now.Sub(tick) <= threshold &&
		!prov.IsZero() && now.Sub(prov) <= threshold &&
		!fj.IsZero() && now.Sub(fj) <= threshold &&
		!o.storageFailed.Load()

	return HealthStatus{
		Healthy:            healthy,
		LastTickAt:         tick,
		LastProviderListAt: prov,
		LastForgejoPollAt:  fj,
		Paused:             o.paused.Load(),
	}
}

// TransportModeCacheGateway is the value of Config.TransportMode that
// selects the FJB-54 cache-as-gateway dispatch path: dial workers by
// VPC IP through the IPsec tunnel terminated on the cache nanode.
//
// Mirrors config.TransportCacheGateway as a literal so this package
// doesn't take a dependency on internal/config.
const TransportModeCacheGateway = "cache-gateway"

// addrFor returns the address the dispatcher should dial for the given
// node, branching on the active transport mode. Empty / "ssh" (default)
// returns the public IPv4 (legacy path); "cache-gateway" (FJB-54)
// returns the VPC IP, routed via the IPsec tunnel.
//
// Defined on Orchestrator (not Node) because the choice is a
// composition-root concern, not a per-node property.
func (o *Orchestrator) addrFor(n *Node) string {
	if o.cfg.TransportMode == TransportModeCacheGateway {
		return n.VPCIP
	}
	return n.IP
}

// addrForInstance is the just-provisioned counterpart of addrFor when
// the caller has a provider.Instance in hand but hasn't yet retrieved
// the Node from the pool. Same selection rule.
func (o *Orchestrator) addrForInstance(ip4, vpcIP string) string {
	if o.cfg.TransportMode == TransportModeCacheGateway {
		return vpcIP
	}
	return ip4
}

func (o *Orchestrator) markTick() {
	o.mu.Lock()
	o.lastTickAt = o.now()
	o.mu.Unlock()
}

func (o *Orchestrator) markProviderList() {
	o.mu.Lock()
	o.lastProviderListAt = o.now()
	o.mu.Unlock()
}

func (o *Orchestrator) markForgejoPoll() {
	o.mu.Lock()
	o.lastForgejoPollAt = o.now()
	o.mu.Unlock()
}
