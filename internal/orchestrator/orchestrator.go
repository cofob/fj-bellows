// Package orchestrator is the always-on daemon: it polls the Forgejo job
// queue, reconciles waiting jobs against Forgejo runners and provider
// instances, provisions/keeps-warm/tears-down worker VMs per the billing
// model, and sweeps orphans. The reconcile loop is the single writer of
// provisioning decisions.
package orchestrator

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/hstern/fj-bellows/internal/bootstrap"
	"github.com/hstern/fj-bellows/internal/control/events"
	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

// JobSource is the slice of the Forgejo API the orchestrator consumes.
// *forgejo.Client satisfies it; tests supply a mock.
type JobSource interface {
	WaitingJobs(ctx context.Context) ([]forgejo.WaitingJob, error)
	RegisterEphemeral(ctx context.Context, name string, labels []string) (forgejo.Registration, error)
	ListRunners(ctx context.Context) ([]forgejo.Runner, error)
	DeleteRunner(ctx context.Context, id int64) error
}

// Config holds the orchestrator's runtime parameters, decoupled from the
// on-disk config struct.
type Config struct {
	Tier                 string
	ProviderName         string
	Driver               string
	Tag                  string
	MaxScale             int
	WarmInstances        int
	Labels               []string
	InstanceType         string
	OneJobPerVM          bool
	ResetMode            string
	ResetMinRemaining    time.Duration
	ResetTimeout         time.Duration
	BootstrapFingerprint string
	ForgejoSource        string
	PollInterval         time.Duration
	RunnerVersion        string
	ReadyFile            string
	Teardown             TeardownPolicy
	AuthorizedKey        string

	// FJBAgentDownloadURL is the fully-resolved URL workers fetch fjbagent
	// from in cloud-init (FJB-94). The agent version implicitly tracks
	// the orchestrator's build (this is the only design that makes sense
	// — agent and orchestrator share a proto). Empty disables agent
	// install entirely.
	FJBAgentDownloadURL string
	// FJBAgentToken is the per-deployment shared-secret bearer token.
	// Required when FJBAgentDownloadURL is set. The orchestrator presents
	// the same token in the Authorization header when dialing the agent.
	FJBAgentToken string

	// TransportMode mirrors config.Transport.Mode (FJB-72). Drives the
	// orchestrator's choice of dial address: empty / "ssh" uses
	// Node.IP (the legacy public IPv4 path); "cache-gateway" (FJB-54)
	// uses Node.VPCIP and assumes an IPsec tunnel exists between the
	// orchestrator and the cache nanode that fronts the worker VPC.
	TransportMode string

	// DrainOnShutdown lets in-flight jobs finish on shutdown instead of being
	// interrupted immediately.
	DrainOnShutdown bool
	// DrainTimeout bounds how long to wait for in-flight jobs when draining;
	// 0 waits indefinitely (rely on the supervisor's stop timeout).
	DrainTimeout time.Duration
	// DestroyOnExit tears down all owned VMs on shutdown. Default false leaves
	// warm VMs for a restarted daemon to readopt; set true for a permanent stop.
	DestroyOnExit bool
}

const phaseOutcomeStorageFailed = "storage_failed"

// hotConfig is an immutable snapshot of the fields that may change without a
// restart. An atomic pointer lets reconcile, control-plane reads, and worker
// goroutines observe a coherent version without racing on Config.
type hotConfig struct {
	MaxScale          int
	WarmInstances     int
	ResetMinRemaining time.Duration
	ResetTimeout      time.Duration
	PollInterval      time.Duration
	RunnerVersion     string
	Teardown          TeardownPolicy
	DrainOnShutdown   bool
	DrainTimeout      time.Duration
	DestroyOnExit     bool
}

// Orchestrator wires the pool, provider, job source, and dispatcher together.
type Orchestrator struct {
	cfg    Config
	prov   provider.Provider
	jobs   JobSource
	disp   Dispatcher
	pool   *Pool
	log    *slog.Logger
	events *events.Bus

	// kick is the out-of-band reconcile-now channel. The control plane sends
	// on this to drive a synchronous reconcile and receive the count summary
	// without waiting on the next ticker tick. Run owns the receiver; only
	// one reconcile ever runs at a time because the ticker and the kick share
	// the same select.
	kick chan kickReq

	// pollReset signals the Run goroutine to recreate its ticker with a new
	// interval after ApplyHotConfig publishes a new atomic hot snapshot.
	// Non-blocking; the latest value wins.
	pollReset chan time.Duration

	wg sync.WaitGroup // tracks in-flight dispatch/provision/teardown goroutines

	// paused suppresses the auto-tick path in Run when true. The kick channel
	// (Reconcile RPC, ForceReap, ForceProvision) ignores this flag — an
	// operator explicitly asking for a tick gets one. Atomic so Pause/Resume
	// don't have to serialise behind Run's mutex.
	paused atomic.Bool
	hot    atomic.Pointer[hotConfig]

	mu          sync.Mutex
	pending     int                 // in-flight provisions not yet in the pool
	builders    int                 // in-flight managed-image builders (capacity only)
	dispatching map[string]struct{} // job handles currently being served
	active      map[string]struct{} // runner UUIDs we registered and still expect
	now         func() time.Time    // injectable clock for tests

	// Freshness timestamps consumed by the control plane's Health endpoint.
	// Each is bumped under mu on success of the corresponding upstream call.
	lastTickAt         time.Time
	lastProviderListAt time.Time
	lastForgejoPollAt  time.Time

	// reapSeen tracks runner UUIDs that looked like zombies last tick; only
	// reaped after two consecutive sightings so a just-registered runner is not
	// deleted in the window before its UUID is recorded. Touched only by the
	// single reconcile goroutine, so it needs no lock.
	reapSeen map[string]struct{}

	// managedImage is the currently active clean image for snapshot-reset
	// tiers. The fleet's image manager swaps it only after a new image is
	// fully created and recorded.
	managedImage     string
	lastImageCheck   time.Time
	lastBuilderCheck time.Time
	imageRefs        map[string]int
	store            storage.Store
	storageFailed    atomic.Bool
}

// New builds an orchestrator.
func New(cfg Config, prov provider.Provider, jobs JobSource, disp Dispatcher, log *slog.Logger) *Orchestrator {
	if log == nil {
		log = slog.Default()
	}
	applyRuntimeDefaults(&cfg)
	o := &Orchestrator{
		cfg:         cfg,
		prov:        prov,
		jobs:        jobs,
		disp:        disp,
		pool:        NewPool(),
		log:         log,
		events:      events.New(),
		kick:        make(chan kickReq),
		pollReset:   make(chan time.Duration, 1),
		dispatching: map[string]struct{}{},
		active:      map[string]struct{}{},
		reapSeen:    map[string]struct{}{},
		imageRefs:   map[string]int{},
		now:         time.Now,
	}
	o.hot.Store(hotConfigFrom(cfg))
	return o
}

func hotConfigFrom(cfg Config) *hotConfig {
	return &hotConfig{
		MaxScale: cfg.MaxScale, WarmInstances: cfg.WarmInstances,
		ResetMinRemaining: cfg.ResetMinRemaining, ResetTimeout: cfg.ResetTimeout,
		PollInterval: cfg.PollInterval, RunnerVersion: cfg.RunnerVersion,
		Teardown: cfg.Teardown, DrainOnShutdown: cfg.DrainOnShutdown,
		DrainTimeout: cfg.DrainTimeout, DestroyOnExit: cfg.DestroyOnExit,
	}
}

func (o *Orchestrator) runtimeConfig() hotConfig {
	if current := o.hot.Load(); current != nil {
		return *current
	}
	return *hotConfigFrom(o.cfg)
}

func applyRuntimeDefaults(cfg *Config) {
	if cfg.ReadyFile == "" {
		cfg.ReadyFile = bootstrap.DefaultReadyFile
	}
	if cfg.ResetMinRemaining == 0 {
		cfg.ResetMinRemaining = 10 * time.Minute
	}
	if cfg.ResetTimeout == 0 {
		cfg.ResetTimeout = 5 * time.Minute
	}
}

// SetStore attaches the process-wide durable ledger. It is intentionally
// separate from Config so hot-reload comparisons never depend on interface
// identity. All tier orchestrators may share one concurrency-safe Store.
func (o *Orchestrator) SetStore(store storage.Store) { o.store = store }

// Run reconciles on each tick until ctx (the shutdown signal) is cancelled,
// then drains in-flight work. Jobs run under an independent context so a
// shutdown can choose to let them finish (drain) rather than interrupt them.
// The kick channel lets the control plane drive a synchronous reconcile out
// of band — the single-writer property is preserved because the kick is
// served from the same goroutine as the ticker.
func (o *Orchestrator) Run(ctx context.Context) error {
	jobCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	t := time.NewTicker(o.runtimeConfig().PollInterval)
	defer t.Stop()
	o.Reconcile(jobCtx)
	for {
		select {
		case <-ctx.Done():
			o.shutdown(cancelJobs)
			return nil
		case <-t.C:
			// While paused the auto-tick is a no-op: the tick is consumed
			// (otherwise the ticker would buffer ticks and burst on resume)
			// but Reconcile is skipped. Kick / ForceReap / ForceProvision
			// still drive a tick when an operator explicitly asks. The
			// freshness counters (LastTickAt, ...) stop advancing so a
			// long-paused daemon's Health goes unhealthy; the `paused`
			// flag on HealthResponse is the signal that this is intentional.
			if o.paused.Load() {
				continue
			}
			o.Reconcile(jobCtx)
		case req := <-o.kick:
			o.serveKick(jobCtx, req)
		case d := <-o.pollReset:
			// ApplyHotConfig changed PollInterval. Recreate the ticker so
			// the new cadence takes effect on the next boundary; the
			// previous one stops and its in-flight tick (if any) is
			// discarded — safe because Reconcile is idempotent.
			t.Stop()
			t = time.NewTicker(d)
			o.log.Info("poll interval reloaded", "interval", d.String())
		}
	}
}

// shutdown stops scheduling new work and waits for in-flight goroutines. With
// DrainOnShutdown it lets running jobs finish (bounded by DrainTimeout, 0 =
// indefinitely); otherwise it interrupts them immediately. Optionally destroys
// owned VMs on exit.
func (o *Orchestrator) shutdown(cancelJobs context.CancelFunc) {
	cfg := o.runtimeConfig()
	if !cfg.DrainOnShutdown {
		o.log.Info("shutting down; interrupting in-flight jobs")
		cancelJobs()
	} else {
		o.log.Info("shutting down; draining in-flight jobs", "timeout", cfg.DrainTimeout.String())
	}

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()

	if cfg.DrainOnShutdown && cfg.DrainTimeout > 0 {
		select {
		case <-done:
		case <-time.After(cfg.DrainTimeout):
			o.log.Warn("drain timeout reached; interrupting remaining jobs")
			cancelJobs()
			<-done
		}
	} else {
		<-done
	}

	if cfg.DestroyOnExit {
		o.destroyAll()
	}
}

// destroyAll tears down every instance currently in the pool, using a fresh
// bounded context since the job context is already cancelled by shutdown.
func (o *Orchestrator) destroyAll() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, n := range o.pool.Snapshot() {
		operationID, err := o.beginStoredMutation(ctx, "destroy_on_exit", n)
		if err != nil {
			o.log.Error("record destroy on exit", "id", n.InstanceID, "err", err)
			continue
		}
		o.finishStoredPhase(ctx, n.PhaseID, "shutdown", "", o.now())
		n.PhaseID = o.startStoredPhase(ctx, n, storage.PhaseRemoving, 0, o.now())
		o.pool.SetPhase(n.InstanceID, n.PhaseID)
		if err := o.prov.Destroy(ctx, n.InstanceID); err != nil {
			o.finishStoredPhase(ctx, n.PhaseID, "failed", err.Error(), o.now())
			o.finishStoredMutation(ctx, operationID, n.InstanceID, "provider_destroy", false)
			o.log.Error("destroy on exit", "id", n.InstanceID, "err", err)
			continue
		}
		destroyedAt := o.now()
		if n.State == StateIdle {
			o.recordIntervalCost(ctx, n, storage.CostWarmIdle, n.LastBusy, destroyedAt)
		}
		o.finishStoredPhase(ctx, n.PhaseID, "destroyed", "", destroyedAt)
		o.finishStoredMutation(ctx, operationID, n.InstanceID, "", true)
		o.closeStoredResource(ctx, n, storage.ResourceClosed, "shutdown")
		o.pool.Delete(n.InstanceID)
		o.log.Info("destroyed on exit", "id", n.InstanceID)
	}
}

// ReconcileResult summarises one convergence pass. Counts are "intents
// started this tick" — async provisions/reaps still need their downstream
// goroutines to finish before the world reflects them.
type ReconcileResult struct {
	Provisioned int      // provisionOne goroutines kicked off
	Dispatched  int      // jobs handed to dispatch goroutines
	Reaped      int      // applyTeardown Destroy actions kicked off
	Adopted     int      // syncPool entries added
	Dropped     int      // syncPool entries removed
	Errors      []string // formatted; one per failing top-level step
}

// kickKind selects which out-of-band action the Run-goroutine performs in
// response to a kickReq. Sharing the kick channel keeps every state mutation
// on a single goroutine.
type kickKind int

const (
	// kickReconcile drives a synchronous Reconcile pass and returns the
	// per-tick summary on req.reconcile.
	kickReconcile kickKind = iota
	// kickForceReap destroys the worker named req.instanceID, bypassing
	// billing policy. Result lands on req.force.
	kickForceReap
	// kickForceProvision spawns one extra worker, bypassing scale.max for
	// this single tick. The new ID lands on req.force.
	kickForceProvision
)

// kickReq is the message the control plane sends on the kick channel to
// drive an out-of-band action from the Run goroutine. Exactly one of
// reconcile or force is populated based on kind.
type kickReq struct {
	kind       kickKind
	instanceID string // populated for kickForceReap
	reconcile  chan ReconcileResult
	force      chan forceResult
}

// forceResult carries the outcome of a force-* kick back to the caller.
// instanceID is populated by kickForceProvision; empty otherwise.
type forceResult struct {
	instanceID string
	err        error
}

// Reconcile performs one convergence pass: sync the pool to provider truth,
// dispatch waiting jobs, provision capacity, and apply teardown. Returns a
// summary the control plane's Reconcile RPC surfaces to operators.
func (o *Orchestrator) Reconcile(ctx context.Context) ReconcileResult {
	var r ReconcileResult
	defer func() {
		o.markTick()
		o.emit("reconcile_tick", map[string]string{
			"provisioned": strconv.Itoa(r.Provisioned),
			"dispatched":  strconv.Itoa(r.Dispatched),
			"reaped":      strconv.Itoa(r.Reaped),
			"adopted":     strconv.Itoa(r.Adopted),
			"dropped":     strconv.Itoa(r.Dropped),
			"errors":      strconv.Itoa(len(r.Errors)),
		})
	}()

	insts, err := o.prov.List(ctx, o.cfg.Tag)
	if err != nil {
		o.log.Error("list instances", "err", err)
		r.Errors = append(r.Errors, "list instances: "+err.Error())
		return r
	}
	o.markProviderList()
	if err := o.recoverOrphanBuilders(ctx); err != nil {
		r.Errors = append(r.Errors, "recover image builders: "+err.Error())
		return r
	}
	r.Adopted, r.Dropped, err = o.syncPool(ctx, insts)
	if err != nil {
		o.storageError("reconcile resources", err)
		r.Errors = append(r.Errors, "storage resources: "+err.Error())
		return r
	}

	jobs, err := o.jobs.WaitingJobs(ctx)
	if err != nil {
		o.log.Error("poll waiting jobs", "err", err)
		r.Errors = append(r.Errors, "poll waiting jobs: "+err.Error())
		jobs = nil
	} else {
		o.markForgejoPoll()
	}
	jobs = filterServiceable(jobs, o.cfg.Labels)
	if err := o.persistObservedJobs(ctx, jobs); err != nil {
		r.Errors = append(r.Errors, "persist jobs: "+err.Error())
		return r
	}

	o.ensureManagedImage(ctx, len(jobs) > 0)
	r.Dispatched, r.Provisioned = o.dispatchJobs(ctx, jobs)
	r.Reaped = o.applyTeardown(ctx)
	o.reapZombieRunners(ctx)
	return r
}

// serveKick dispatches one out-of-band request from the Run-goroutine. The
// single-writer property of the reconcile loop is preserved because every
// pool mutation here happens on the same goroutine as the ticker.
func (o *Orchestrator) serveKick(jobCtx context.Context, req kickReq) {
	switch req.kind {
	case kickReconcile:
		req.reconcile <- o.Reconcile(jobCtx)
	case kickForceReap:
		req.force <- forceResult{err: o.doForceReap(jobCtx, req.instanceID)}
	case kickForceProvision:
		req.force <- o.doForceProvision(jobCtx)
	default:
		// Unreachable in practice; keep the runtime defensive so a future
		// kind added without a case here can't silently wedge the caller.
		if req.reconcile != nil {
			req.reconcile <- ReconcileResult{Errors: []string{fmt.Sprintf("unknown kick kind: %d", req.kind)}}
		}
		if req.force != nil {
			req.force <- forceResult{err: fmt.Errorf("unknown kick kind: %d", req.kind)}
		}
	}
}

// ForceReap immediately destroys the worker with the given instance ID
// even if billing policy would keep it warm. Cancels any in-flight teardown
// state and runs provider.Destroy. Drops the node from the pool on success.
// Audit-logged with the caller identity threaded via WithAuditCaller.
// Returns an error if the instance isn't in the pool or Destroy fails.
//
// Must only be invoked when Run is active; the kick is served from the Run
// goroutine. Without Run, the call returns "orchestrator not running".
func (o *Orchestrator) ForceReap(ctx context.Context, instanceID string) error {
	o.log.Info("force-reap requested", "id", instanceID, "caller", auditCallerFromCtx(ctx))
	if o.kick == nil {
		return errors.New("orchestrator not running (no kick channel)")
	}
	resultCh := make(chan forceResult, 1)
	req := kickReq{kind: kickForceReap, instanceID: instanceID, force: resultCh}
	select {
	case o.kick <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case r := <-resultCh:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ForceProvision spawns one extra worker, bypassing scale.max for this
// single tick. Audit-logged. Returns the new instance ID on success, or an
// error if Provision fails immediately (async WaitReady errors surface later
// as worker_reaped events on the StreamEvents stream).
func (o *Orchestrator) ForceProvision(ctx context.Context) (string, error) {
	o.log.Info("force-provision requested", "caller", auditCallerFromCtx(ctx))
	if o.kick == nil {
		return "", errors.New("orchestrator not running (no kick channel)")
	}
	resultCh := make(chan forceResult, 1)
	req := kickReq{kind: kickForceProvision, force: resultCh}
	select {
	case o.kick <- req:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	select {
	case r := <-resultCh:
		return r.instanceID, r.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Pause stops the reconcile loop from auto-ticking. In-flight dispatch /
// provision / teardown goroutines continue. Subsequent ticker ticks become
// no-ops; explicit Kick / ForceReap / ForceProvision requests still run.
// Idempotent: pausing an already-paused orchestrator is a no-op.
//
// Audit-logged with the caller identity threaded via WithAuditCaller; the
// control plane handler builds the identity from the Connect request peer
// (and bearer-token presence) before invoking this.
func (o *Orchestrator) Pause(ctx context.Context) {
	// CompareAndSwap so the log line only fires on the actual transition;
	// idempotent re-pauses stay silent.
	if o.paused.CompareAndSwap(false, true) {
		o.log.Info("paused", "caller", auditCallerFromCtx(ctx))
		o.emit("reconciler_paused", map[string]string{attrCaller: auditCallerFromCtx(ctx)})
	}
}

// Resume re-arms the auto-ticker. Idempotent. Audit-logged.
func (o *Orchestrator) Resume(ctx context.Context) {
	if o.paused.CompareAndSwap(true, false) {
		o.log.Info("resumed", "caller", auditCallerFromCtx(ctx))
		o.emit("reconciler_resumed", map[string]string{attrCaller: auditCallerFromCtx(ctx)})
	}
}

// IsPaused reports the current pause flag.
func (o *Orchestrator) IsPaused() bool {
	return o.paused.Load()
}

// doForceReap runs the synchronous Destroy from the Run goroutine. The
// node is transitioned to StateRemoving (overriding any prior state) before
// Destroy is called so a concurrent applyTeardown can't pick it up; on
// success the pool is updated. On Destroy failure the node is left in
// StateRemoving and the next reconcile's teardown path will retry it via
// the normal idle-retry behaviour.
func (o *Orchestrator) doForceReap(ctx context.Context, instanceID string) error {
	n, ok := o.pool.Get(instanceID)
	if !ok {
		return fmt.Errorf("instance %q not in pool", instanceID)
	}
	// Force into StateRemoving so applyTeardown / dispatch concurrent paths
	// won't act on this node. SetState returns false only when the node
	// has been deleted between Get and SetState — treat that as "already
	// reaped by someone" and surface a clean error.
	if !o.pool.SetState(instanceID, StateRemoving) {
		return fmt.Errorf("instance %q vanished from pool", instanceID)
	}
	operationID, err := o.beginStoredMutation(ctx, "force_destroy", n)
	if err != nil {
		o.pool.SetState(instanceID, n.State)
		return err
	}
	phaseAt := o.now()
	o.finishStoredPhase(ctx, n.PhaseID, "force_reap", "", phaseAt)
	n.PhaseID = o.startStoredPhase(ctx, n, storage.PhaseRemoving, 0, phaseAt)
	o.pool.SetPhase(instanceID, n.PhaseID)
	if err := o.prov.Destroy(ctx, instanceID); err != nil {
		o.finishStoredPhase(ctx, n.PhaseID, "failed", err.Error(), o.now())
		o.finishStoredMutation(ctx, operationID, instanceID, "provider_destroy", false)
		o.log.Error("force-reap destroy", "id", instanceID, "err", err)
		// Drop back to Idle so the next teardown tick (or another force-reap)
		// can retry. Reaping a node twice is harmless — provider.Destroy is
		// idempotent.
		o.pool.SetState(instanceID, n.State)
		if o.store != nil && n.ResourceID != 0 && n.State != StateDraining && n.State != StateResetting {
			if stateErr := o.store.SetResourceState(ctx, n.ResourceID, storage.ResourceActive, o.now()); stateErr != nil {
				o.storageError("restore force-reap resource", stateErr)
			}
		}
		n.PhaseID = o.startStoredPhase(ctx, n, phaseKindForState(n.State), 0, o.now())
		o.pool.SetPhase(instanceID, n.PhaseID)
		return fmt.Errorf("destroy %s: %w", instanceID, err)
	}
	destroyedAt := o.now()
	if n.State == StateIdle {
		o.recordIntervalCost(ctx, n, storage.CostWarmIdle, n.LastBusy, destroyedAt)
	}
	o.finishStoredPhase(ctx, n.PhaseID, "destroyed", "", destroyedAt)
	o.finishStoredMutation(ctx, operationID, instanceID, "", true)
	o.closeStoredResource(ctx, n, storage.ResourceClosed, "force_reap")
	o.pool.Delete(instanceID)
	o.log.Info("force-reaped worker", "id", instanceID, "ip", n.IP)
	o.emit("worker_reaped", map[string]string{attrID: instanceID, attrIP: n.IP})
	return nil
}

// doForceProvision spawns one extra worker, bypassing scale.max. Runs
// provider.Provision synchronously from the Run goroutine so the caller
// receives the new instance ID before returning; WaitReady is then
// off-loaded to a wg goroutine the same way provisionOne does, so the
// daemon doesn't block its reconcile loop on a slow boot.
//
//nolint:gocyclo // Provisioning compensation remains linear and mirrors the normal provision path stage for stage.
func (o *Orchestrator) doForceProvision(ctx context.Context) forceResult {
	runtime := o.runtimeConfig()
	pinner, canPin := o.disp.(HostKeyPinner)
	var hostPriv string
	var sshHostPub ssh.PublicKey
	if canPin {
		var err error
		hostPriv, sshHostPub, err = generateHostKey()
		if err != nil {
			o.log.Error("force-provision generate host key", "err", err)
			return forceResult{err: fmt.Errorf("generate host key: %w", err)}
		}
	}
	userData, err := bootstrap.Render(bootstrap.Params{
		RunnerVersion:       runtime.RunnerVersion,
		ReadyFile:           o.cfg.ReadyFile,
		HostPrivateKey:      hostPriv,
		FJBAgentDownloadURL: o.cfg.FJBAgentDownloadURL,
		FJBAgentToken:       o.cfg.FJBAgentToken,
	})
	if err != nil {
		o.log.Error("force-provision render cloud-init", "err", err)
		return forceResult{err: fmt.Errorf("render cloud-init: %w", err)}
	}
	name := o.cfg.Tag + "-" + shortID()
	resource, phase, err := o.beginStoredResource(ctx, name, name)
	if err != nil {
		return forceResult{err: err}
	}
	spec := provider.Spec{
		Tag:           o.cfg.Tag,
		Name:          name,
		Role:          "worker",
		InstanceType:  o.cfg.InstanceType,
		ImageID:       o.managedImageID(),
		UserData:      userData,
		AuthorizedKey: o.cfg.AuthorizedKey,
		Labels:        o.cfg.Labels,
	}
	inst, err := o.prov.Provision(ctx, spec)
	if err != nil {
		o.log.Error("force-provision", "err", err)
		o.failStoredProvision(ctx, resource, phase.ID, "provider_create")
		return forceResult{err: fmt.Errorf("provision: %w", err)}
	}
	generation, storageErr := o.activateStoredResource(ctx, resource, inst)
	if storageErr != nil {
		o.cleanupFailedActivation(ctx, resource, phase.ID, inst, "storage_activation")
		return forceResult{err: storageErr}
	}
	o.pool.Put(&Node{
		InstanceID:   inst.ID,
		ResourceID:   resource.ID,
		GenerationID: generation.ID,
		PriceQuoteID: resource.PriceQuoteID,
		PhaseID:      phase.ID,
		State:        StateProvisioning,
		IP:           inst.IPv4,
		VPCIP:        inst.VPCIPv4,
		CreatedAt:    inst.CreatedAt,
		LastBusy:     o.now(),
	})
	o.log.Info("force-provisioned", "id", inst.ID, "ip", inst.IPv4)
	o.emit("worker_provisioned", map[string]string{attrID: inst.ID, attrIP: inst.IPv4})

	// Seed the pinned host key before the first dial so WaitReady's
	// handshake is verified, then push WaitReady off the reconcile
	// goroutine — identical to the in-band provisionOne path.
	if canPin {
		pinner.PinHostKey(inst.IPv4, sshHostPub)
	}
	id, ip := inst.ID, inst.IPv4
	dialAddr := o.addrForInstance(inst.IPv4, inst.VPCIPv4)
	o.wg.Go(func() {
		if err := o.disp.WaitReady(ctx, id, dialAddr); err != nil {
			o.log.Error("force-provision worker readiness", "id", id, "err", err)
			o.finishStoredPhase(ctx, phase.ID, "failed", "readiness", o.now())
			if o.store != nil && generation.ID != 0 {
				_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
			}
			o.pool.SetState(id, StateDraining)
			return
		}
		readyAt := o.now()
		current, exists := o.pool.Get(id)
		if exists {
			o.recordIntervalCost(ctx, current, storage.CostBoot, current.CreatedAt, readyAt)
			o.finishStoredPhase(ctx, current.PhaseID, "ready", "", readyAt)
		}
		if o.store != nil && generation.ID != 0 {
			if err := o.store.SetGenerationState(ctx, generation.ID, storage.GenerationReady, readyAt); err != nil {
				o.storageError("ready force-provision generation", err)
				o.pool.SetState(id, StateDraining)
				return
			}
		}
		if exists {
			phaseID := o.startStoredPhase(ctx, current, storage.PhaseReadyIdle, 0, readyAt)
			o.pool.SetPhase(id, phaseID)
		}
		if !o.pool.SetState(id, StateIdle) {
			return
		}
		o.pool.Touch(id, readyAt)
		o.log.Info("force-provisioned worker ready", "id", id)
		o.emit("worker_ready", map[string]string{attrID: id, attrIP: ip})
	})
	return forceResult{instanceID: inst.ID}
}

// reapZombieRunners deletes runner registrations that are ours (name carries
// the tag prefix) but that we are no longer running a job for — e.g. a VM that
// died after registering but before one-job completed, leaving a dangling
// registration Forgejo never auto-removed. A runner must look orphaned for two
// consecutive ticks before deletion, closing the race against a runner whose
// UUID we have not recorded as active yet.
func (o *Orchestrator) reapZombieRunners(ctx context.Context) {
	runners, err := o.jobs.ListRunners(ctx)
	if err != nil {
		o.log.Error("list runners", "err", err)
		return
	}
	prefix := o.cfg.Tag + "-"
	seen := map[string]struct{}{}
	for _, r := range runners {
		if !strings.HasPrefix(r.Name, prefix) || o.isActive(r.UUID) {
			continue
		}
		if _, twice := o.reapSeen[r.UUID]; !twice {
			seen[r.UUID] = struct{}{} // first sighting; revisit next tick
			continue
		}
		if err := o.jobs.DeleteRunner(ctx, r.ID); err != nil {
			o.log.Error("reap zombie runner", "uuid", r.UUID, "name", r.Name, "err", err)
			seen[r.UUID] = struct{}{} // keep trying next tick
			continue
		}
		o.log.Info("reaped zombie runner", "uuid", r.UUID, "name", r.Name)
		o.emit("zombie_reaped", map[string]string{attrUUID: r.UUID, attrName: r.Name})
	}
	// ListRunners reaching this point means the Forgejo call succeeded above;
	// bump the freshness signal alongside WaitingJobs.
	o.markForgejoPoll()
	o.reapSeen = seen
}

// syncPool adopts provider instances unknown to the pool (crash recovery) and
// drops pool nodes the provider no longer reports. Provisioning nodes are never
// dropped: a freshly created VM may not appear in List yet. Returns the count
// of nodes adopted and dropped this tick.
//
//nolint:gocyclo,nestif // Reconcile recovery is an explicit provider/resource/generation state matrix.
func (o *Orchestrator) syncPool(ctx context.Context, insts []provider.Instance) (adopted, dropped int, err error) {
	now := o.now()
	resources, generations, err := o.storageResources(ctx)
	if err != nil {
		return 0, 0, err
	}
	seen := map[string]struct{}{}
	for _, in := range insts {
		seen[in.ID] = struct{}{}
		if _, ok := o.pool.Get(in.ID); !ok {
			resource := resources[in.ID]
			if resource.ID == 0 {
				resource = resources["name:"+in.Name]
			}
			generation := generations[resource.ID]
			if generation.State == storage.GenerationPreparing && o.pendingCount() > 0 {
				// The provision goroutine has durably activated this resource but
				// has not put it in the pool yet. Let that goroutine finish rather
				// than conservatively adopting its half-booted generation.
				continue
			}
			if o.store != nil && resource.ID != 0 && resource.ExternalID == "" {
				generation, err = o.activateStoredResource(ctx, resource, in)
				if err != nil {
					return adopted, dropped, err
				}
				resource.ExternalID = in.ID
				resource.ProviderCreatedAt = in.CreatedAt
			} else if o.store != nil && resource.ID == 0 {
				resource, generation, err = o.adoptStoredResource(ctx, in)
				if err != nil {
					return adopted, dropped, err
				}
			}
			state := StateIdle
			if (o.store != nil && resource.ID != 0 && generation.ID == 0) ||
				resource.State == storage.ResourceResetting || resource.State == storage.ResourceDestroying ||
				generation.State == storage.GenerationPreparing || generation.State == storage.GenerationInUse ||
				generation.State == storage.GenerationDirty {
				// Missing generation state, or a reboot/reset that was in progress
				// when the daemon died, has no trustworthy cleanliness proof. Drain
				// it through normal teardown instead of exposing it to dispatch.
				state = StateDraining
			}
			lastBusy := now
			if state == StateIdle && !generation.ReadyAt.IsZero() {
				lastBusy = generation.ReadyAt
			}
			node := Node{
				InstanceID:   in.ID,
				ResourceID:   resource.ID,
				GenerationID: generation.ID,
				PriceQuoteID: resource.PriceQuoteID,
				State:        state,
				IP:           in.IPv4,
				VPCIP:        in.VPCIPv4,
				CreatedAt:    in.CreatedAt,
				LastBusy:     lastBusy,
			}
			phaseKind := storage.PhaseReadyIdle
			if state == StateDraining {
				phaseKind = storage.PhaseRemoving
			}
			node.PhaseID = o.startStoredPhase(ctx, node, phaseKind, 0, now)
			o.pool.Put(&node)
			o.log.Info("adopted orphan instance", "id", in.ID, "ip", in.IPv4)
			o.emit("worker_adopted", map[string]string{attrID: in.ID, attrIP: in.IPv4})
			adopted++
		}
	}
	if o.store != nil {
		for externalID, resource := range resources {
			if strings.HasPrefix(externalID, "name:") || resource.ExternalID == "" {
				continue
			}
			if _, ok := seen[externalID]; ok {
				continue
			}
			if current, ok := o.pool.Get(externalID); ok && current.State == StateProvisioning {
				continue
			}
			generation := generations[resource.ID]
			if generation.State == storage.GenerationPreparing && o.pendingCount() > 0 {
				continue
			}
			if generation.ID != 0 {
				_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, now)
			}
			if _, tracked := o.pool.Get(externalID); !tracked {
				o.recordResourceCost(ctx, Node{
					InstanceID: externalID, ResourceID: resource.ID, PriceQuoteID: resource.PriceQuoteID,
					CreatedAt: resource.ProviderCreatedAt,
				}, storage.CostBilledCompute, now)
			}
			if err := o.store.CloseResource(ctx, resource.ID, storage.ResourceVanished, "provider_list_missing", now); err != nil {
				return adopted, dropped, err
			}
		}
		if err := o.recoverPendingMutations(ctx, seen, resources, generations); err != nil {
			return adopted, dropped, err
		}
	}
	for _, n := range o.pool.Snapshot() {
		if _, ok := seen[n.InstanceID]; ok {
			continue
		}
		if n.State == StateProvisioning {
			continue
		}
		o.pool.Delete(n.InstanceID)
		o.closeStoredResource(ctx, n, storage.ResourceVanished, "provider_list_missing")
		o.log.Info("dropped vanished instance", "id", n.InstanceID, "state", n.State)
		o.emit("worker_dropped", map[string]string{attrID: n.InstanceID, attrState: string(n.State)})
		dropped++
	}
	return adopted, dropped, nil
}

// dispatchJobs assigns waiting jobs to idle nodes and provisions capacity for
// the rest, bounded by MaxScale. Returns the count of dispatches and
// provisions kicked off this tick.
func (o *Orchestrator) dispatchJobs(ctx context.Context, jobs []forgejo.WaitingJob) (dispatched, provisioned int) {
	runtime := o.runtimeConfig()
	idle := o.pool.ByState(StateIdle)
	next := 0
	waitingWithoutIdle := 0
	optimizationWaiting := 0
	optimizer, _ := o.jobs.(interface {
		HoldForRoutingOptimization(string) bool
	})
	for _, job := range jobs {
		if o.isDispatching(job.Handle) {
			continue
		}
		if next < len(idle) {
			if o.dispatch(ctx, idle[next], job) {
				dispatched++
			}
			next++
			continue
		}
		waitingWithoutIdle++
		if optimizer != nil && optimizer.HoldForRoutingOptimization(job.Handle) {
			optimizationWaiting++
		}
	}
	// Credit in-flight new capacity against unmet demand: nodes that are still
	// booting (StateProvisioning) or whose Provision call has not yet landed in
	// the pool (pending) will become Idle without us spawning anything new.
	// Without this credit, a slow boot (boot_time >> poll_interval) re-evaluates
	// "I have N waiting and 0 idle" every poll and stamps out ~ceil(boot/poll)×
	// VMs per real job until MaxScale caps. See #32.
	soon := len(o.pool.ByState(StateProvisioning)) + len(o.pool.ByState(StateResetting)) + o.pendingCount()
	remainingIdle := len(idle) - next
	// Maintain the configured idle reserve in addition to unmet jobs. Busy
	// workers never satisfy warm capacity; provisioning workers count once as
	// future capacity so a slow boot cannot trigger duplicate creates.
	needProvision := waitingWithoutIdle - optimizationWaiting + runtime.WarmInstances - remainingIdle - soon
	if needProvision <= 0 {
		return dispatched, provisioned
	}
	// MaxScale stays as the final safety net; the credit above is the primary
	// guard so we no longer rely on it to stop runaway provisioning.
	active := o.pool.Len() + o.pendingCount() + o.builderCount()
	canAdd := runtime.MaxScale - active
	for i := 0; i < needProvision && i < canAdd; i++ {
		o.provisionOne(ctx)
		provisioned++
	}
	return dispatched, provisioned
}

// dispatch marks a node Busy and serves the job in a goroutine. Returns
// true when a goroutine was spawned (i.e. the handle wasn't already in
// flight); the caller increments its dispatch counter on true.
//
//nolint:gocyclo // Job execution and its durable cleanup stages intentionally share one goroutine lifecycle.
func (o *Orchestrator) dispatch(ctx context.Context, node Node, job forgejo.WaitingJob) bool {
	if !o.markDispatching(job.Handle) {
		return false
	}
	assignedAt := o.now()
	storedJob, err := o.persistJob(ctx, node, job, storage.JobAssigned, "", assignedAt)
	if err != nil {
		o.unmarkDispatching(job.Handle)
		return false
	}
	if o.store != nil && node.GenerationID != 0 {
		if err := o.store.SetGenerationState(ctx, node.GenerationID, storage.GenerationInUse, assignedAt); err != nil {
			o.storageError("mark generation in use", err)
			o.unmarkDispatching(job.Handle)
			return false
		}
	}
	o.recordIntervalCost(ctx, node, storage.CostWarmIdle, node.LastBusy, assignedAt)
	o.finishStoredPhase(ctx, node.PhaseID, "dispatched", "", assignedAt)
	node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseJob, storedJob.ID, assignedAt)
	o.pool.SetState(node.InstanceID, StateBusy)
	o.pool.SetJob(node.InstanceID, job.Handle)
	o.pool.SetPhase(node.InstanceID, node.PhaseID)
	o.emit("worker_busy", map[string]string{attrID: node.InstanceID, attrIP: node.IP, attrHandle: job.Handle})
	o.wg.Go(func() {
		runtime := o.runtimeConfig()
		phaseOutcome := "interrupted"
		defer func() {
			cleanupTimeout := max(2*time.Minute, runtime.ResetTimeout+time.Minute)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			defer cancel()
			current, ok := o.pool.Get(node.InstanceID)
			if !ok {
				current = node
			}
			o.finishStoredPhase(cleanupCtx, current.PhaseID, phaseOutcome, "", o.now())
			generationPersisted := true
			if o.store != nil && current.GenerationID != 0 {
				state := storage.GenerationReady
				if o.cfg.OneJobPerVM {
					state = storage.GenerationDirty
				}
				if err := o.store.SetGenerationState(cleanupCtx, current.GenerationID, state, o.now()); err != nil {
					o.storageError("finish job generation", err)
					generationPersisted = false
				}
			}
			o.pool.SetJob(node.InstanceID, "")
			o.pool.Touch(node.InstanceID, o.now())
			o.unmarkDispatching(job.Handle)
			if !generationPersisted {
				o.destroyAfterJob(cleanupCtx, current, "generation_persistence_failed")
				return
			}
			o.finishDispatchedWorker(cleanupCtx, current)
		}()
		name := o.cfg.Tag + "-" + shortID()
		reg, err := o.jobs.RegisterEphemeral(ctx, name, o.cfg.Labels)
		if err != nil {
			phaseOutcome = "infra_failed"
			o.log.Error("register ephemeral runner", "err", err)
			terminalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			stored, persistErr := o.persistJob(terminalCtx, node, job, storage.JobInfraFailed, "registration", o.now())
			if persistErr != nil {
				phaseOutcome = phaseOutcomeStorageFailed
			} else {
				o.recordDirectJobCost(terminalCtx, node, stored)
			}
			cancel()
			o.emit("job_failed", map[string]string{attrID: node.InstanceID, attrHandle: job.Handle, "failure_class": "registration"})
			return
		}
		o.addActive(reg.UUID)
		defer o.removeActive(reg.UUID)
		o.emit("job_dispatched", map[string]string{attrID: node.InstanceID, attrIP: node.IP, attrHandle: job.Handle, attrRunnerUUID: reg.UUID})
		if _, err := o.persistJob(ctx, node, job, storage.JobRunning, "", o.now()); err != nil {
			phaseOutcome = phaseOutcomeStorageFailed
			return
		}
		if err := o.disp.RunJob(ctx, node.InstanceID, o.addrFor(&node), reg, job); err != nil {
			phaseOutcome = "infra_failed"
			o.log.Error("run job", "handle", job.Handle, "ip", node.IP, "err", err)
			terminalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			stored, persistErr := o.persistJob(terminalCtx, node, job, storage.JobInfraFailed, "runner", o.now())
			o.enrichJob(terminalCtx, job, storage.JobInfraFailed)
			if persistErr != nil {
				phaseOutcome = phaseOutcomeStorageFailed
			} else {
				o.recordDirectJobCost(terminalCtx, node, stored)
			}
			cancel()
			o.emit("job_failed", map[string]string{attrID: node.InstanceID, attrIP: node.IP, attrHandle: job.Handle, "failure_class": "runner"})
			return
		}
		o.log.Info("job complete", "handle", job.Handle, "ip", node.IP)
		phaseOutcome = "succeeded"
		terminalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		stored, persistErr := o.persistJob(terminalCtx, node, job, storage.JobSucceeded, "", o.now())
		o.enrichJob(terminalCtx, job, storage.JobSucceeded)
		if persistErr != nil {
			phaseOutcome = phaseOutcomeStorageFailed
		} else {
			o.recordDirectJobCost(terminalCtx, node, stored)
		}
		cancel()
		o.emit("job_complete", map[string]string{attrID: node.InstanceID, attrIP: node.IP, attrHandle: job.Handle})
	})
	return true
}

// finishDispatchedWorker enforces the tier's cleanliness contract after every
// attempt, including registration and runner failures.
//
//nolint:nestif // The reset capability fallback mirrors the one-job cleanup decision tree directly.
func (o *Orchestrator) finishDispatchedWorker(ctx context.Context, node Node) {
	if !o.cfg.OneJobPerVM {
		idleAt := o.now()
		node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseReadyIdle, 0, idleAt)
		o.pool.SetPhase(node.InstanceID, node.PhaseID)
		o.pool.SetState(node.InstanceID, StateIdle)
		o.emit("worker_idle", map[string]string{attrID: node.InstanceID, attrIP: node.IP})
		return
	}
	if o.cfg.ResetMode == resetModeSnapshot {
		imageID := o.acquireManagedImage()
		if imageID != "" {
			if resetter, ok := o.prov.(provider.Resetter); ok {
				if o.resetWorker(ctx, node, resetter, imageID) {
					return
				}
			} else {
				o.releaseImage(imageID)
			}
		}
	}
	o.destroyAfterJob(ctx, node, "one_job_complete")
}

//nolint:gocyclo,funlen // Reset has ordered compensation for every external and durable stage.
func (o *Orchestrator) resetWorker(ctx context.Context, node Node, resetter provider.Resetter, imageID string) bool {
	defer o.releaseImage(imageID)
	runtime := o.runtimeConfig()
	now := o.now()
	if runtime.Teardown.Model == provider.BillingHourlyRoundUp {
		timing := runtime.Teardown.Timing(node, now)
		if !timing.ReapEligibleAt.IsZero() && timing.ReapEligibleAt.Sub(now) < runtime.ResetMinRemaining {
			o.emit("worker_reset_skipped", map[string]string{attrID: node.InstanceID, attrReason: "billing_boundary"})
			return false
		}
	}
	pinner, canPin := o.disp.(HostKeyPinner)
	var hostPriv string
	var hostPub ssh.PublicKey
	if canPin {
		var err error
		hostPriv, hostPub, err = generateHostKey()
		if err != nil {
			o.log.Error("generate reset host key", "id", node.InstanceID, "err", err)
			return false
		}
	}
	userData, err := bootstrap.Render(bootstrap.Params{
		RunnerVersion:       runtime.RunnerVersion,
		ReadyFile:           o.cfg.ReadyFile,
		HostPrivateKey:      hostPriv,
		FJBAgentDownloadURL: o.cfg.FJBAgentDownloadURL,
		FJBAgentToken:       o.cfg.FJBAgentToken,
	})
	if err != nil {
		o.log.Error("render reset cloud-init", "id", node.InstanceID, "err", err)
		return false
	}
	generation, err := o.beginResetGeneration(ctx, node, imageID)
	if err != nil {
		return false
	}
	node.GenerationID = generation.ID
	if !o.pool.SetState(node.InstanceID, StateResetting) {
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
		}
		return false
	}
	resetStarted := o.now()
	node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseReset, 0, resetStarted)
	o.pool.SetPhase(node.InstanceID, node.PhaseID)
	o.emit("worker_resetting", map[string]string{attrID: node.InstanceID, attrIP: node.IP})
	resetCtx, cancel := context.WithTimeout(ctx, runtime.ResetTimeout)
	defer cancel()
	inst, err := resetter.Reset(resetCtx, node.InstanceID, provider.ResetSpec{
		ImageID:       imageID,
		UserData:      userData,
		AuthorizedKey: o.cfg.AuthorizedKey,
	})
	if err != nil {
		o.recordIntervalCost(ctx, node, storage.CostReset, resetStarted, o.now())
		o.finishStoredPhase(ctx, node.PhaseID, "failed", err.Error(), o.now())
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
		}
		o.log.Error("reset worker", "id", node.InstanceID, "err", err)
		o.emit("worker_reset_failed", map[string]string{attrID: node.InstanceID})
		return false
	}
	if inst.ID != "" && inst.ID != node.InstanceID {
		o.recordIntervalCost(ctx, node, storage.CostReset, resetStarted, o.now())
		o.finishStoredPhase(ctx, node.PhaseID, "failed", "instance_id_changed", o.now())
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
		}
		o.log.Error("reset changed instance id", "old_id", node.InstanceID, "new_id", inst.ID)
		return false
	}
	ip := inst.IPv4
	if ip == "" {
		ip = node.IP
	}
	vpcIP := inst.VPCIPv4
	if vpcIP == "" {
		vpcIP = node.VPCIP
	}
	if canPin {
		pinner.PinHostKey(ip, hostPub)
	}
	if err := o.disp.WaitReady(resetCtx, node.InstanceID, o.addrForInstance(ip, vpcIP)); err != nil {
		o.recordIntervalCost(ctx, node, storage.CostReset, resetStarted, o.now())
		o.finishStoredPhase(ctx, node.PhaseID, "failed", "readiness", o.now())
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
		}
		o.log.Error("reset worker readiness", "id", node.InstanceID, "err", err)
		o.emit("worker_reset_failed", map[string]string{attrID: node.InstanceID, attrStage: "readiness"})
		return false
	}
	resetFinished := o.now()
	o.recordIntervalCost(ctx, node, storage.CostReset, resetStarted, resetFinished)
	o.finishStoredPhase(ctx, node.PhaseID, "ready", "", resetFinished)
	if o.store != nil && generation.ID != 0 {
		if err := o.store.SetGenerationState(ctx, generation.ID, storage.GenerationReady, o.now()); err != nil {
			o.storageError("ready reset generation", err)
			return false
		}
		if err := o.store.SetResourceState(ctx, node.ResourceID, storage.ResourceActive, o.now()); err != nil {
			o.storageError("activate reset resource", err)
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
			return false
		}
	}
	node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseReadyIdle, 0, resetFinished)
	if !o.pool.ResetReady(node.InstanceID, ip, vpcIP, generation.ID, resetFinished) {
		o.finishStoredPhase(ctx, node.PhaseID, "failed", "worker_missing_after_reset", o.now())
		if o.store != nil && generation.ID != 0 {
			_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
		}
		return false
	}
	o.pool.SetPhase(node.InstanceID, node.PhaseID)
	o.emit("worker_reset", map[string]string{attrID: node.InstanceID, attrIP: ip})
	o.emit("worker_idle", map[string]string{attrID: node.InstanceID, attrIP: ip})
	return true
}

func (o *Orchestrator) destroyAfterJob(ctx context.Context, node Node, reason string) {
	if !o.pool.SetState(node.InstanceID, StateRemoving) {
		return
	}
	operationID, err := o.beginStoredMutation(ctx, "destroy", node)
	if err != nil {
		o.pool.SetState(node.InstanceID, StateDraining)
		return
	}
	node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseRemoving, 0, o.now())
	o.pool.SetPhase(node.InstanceID, node.PhaseID)
	if err := o.prov.Destroy(ctx, node.InstanceID); err != nil {
		o.finishStoredPhase(ctx, node.PhaseID, "failed", err.Error(), o.now())
		o.finishStoredMutation(ctx, operationID, node.InstanceID, "provider_destroy", false)
		o.log.Error("destroy one-job worker", "id", node.InstanceID, "err", err)
		o.pool.SetState(node.InstanceID, StateDraining)
		o.emit("worker_reap_failed", map[string]string{attrID: node.InstanceID, attrReason: reason})
		return
	}
	o.finishStoredPhase(ctx, node.PhaseID, "destroyed", "", o.now())
	o.finishStoredMutation(ctx, operationID, node.InstanceID, "", true)
	o.closeStoredResource(ctx, node, storage.ResourceClosed, reason)
	o.pool.Delete(node.InstanceID)
	o.emit("worker_reaped", map[string]string{attrID: node.InstanceID, attrIP: node.IP, attrReason: reason})
}

// provisionOne creates a VM, adds it as Provisioning, waits for readiness, then
// marks it Idle. It counts as pending until it lands in the pool so concurrent
// reconciles do not over-provision.
//
//nolint:gocyclo // Provisioning keeps all compensation branches adjacent to their external side effects.
func (o *Orchestrator) provisionOne(ctx context.Context) {
	o.incPending()
	o.wg.Go(func() {
		runtime := o.runtimeConfig()
		name := o.cfg.Tag + "-" + shortID()
		resource, phase, err := o.beginStoredResource(ctx, name, name)
		if err != nil {
			o.decPending()
			return
		}
		// When the dispatcher can pre-pin host keys, generate a fresh ed25519 SSH
		// host key per VM and inject its private half via cloud-init so the worker
		// presents exactly this key; the public half is pinned after Provision so
		// even the first dial is verified. A dispatcher without host keys (e.g. a
		// docker-exec one) skips this and renders without a host key.
		pinner, canPin := o.disp.(HostKeyPinner)
		var hostPriv string
		var sshHostPub ssh.PublicKey
		if canPin {
			var err error
			hostPriv, sshHostPub, err = generateHostKey()
			if err != nil {
				o.log.Error("generate worker host key", "err", err)
				o.failStoredProvision(ctx, resource, phase.ID, "generate_host_key")
				o.decPending()
				return
			}
		}
		userData, err := bootstrap.Render(bootstrap.Params{
			RunnerVersion:       runtime.RunnerVersion,
			ReadyFile:           o.cfg.ReadyFile,
			HostPrivateKey:      hostPriv,
			FJBAgentDownloadURL: o.cfg.FJBAgentDownloadURL,
			FJBAgentToken:       o.cfg.FJBAgentToken,
		})
		if err != nil {
			o.log.Error("render cloud-init", "err", err)
			o.failStoredProvision(ctx, resource, phase.ID, "render_cloud_init")
			o.decPending()
			return
		}
		spec := provider.Spec{
			Tag:           o.cfg.Tag,
			Name:          name,
			Role:          "worker",
			InstanceType:  o.cfg.InstanceType,
			ImageID:       o.managedImageID(),
			UserData:      userData,
			AuthorizedKey: o.cfg.AuthorizedKey,
			Labels:        o.cfg.Labels,
		}
		inst, err := o.prov.Provision(ctx, spec)
		if err != nil {
			o.log.Error("provision", "err", err)
			o.failStoredProvision(ctx, resource, phase.ID, "provider_create")
			o.decPending()
			return
		}
		generation, storageErr := o.activateStoredResource(ctx, resource, inst)
		if storageErr != nil {
			o.cleanupFailedActivation(ctx, resource, phase.ID, inst, "storage_activation")
			o.decPending()
			return
		}
		o.pool.Put(&Node{
			InstanceID:   inst.ID,
			ResourceID:   resource.ID,
			GenerationID: generation.ID,
			PriceQuoteID: resource.PriceQuoteID,
			PhaseID:      phase.ID,
			State:        StateProvisioning,
			IP:           inst.IPv4,
			VPCIP:        inst.VPCIPv4,
			CreatedAt:    inst.CreatedAt,
			LastBusy:     o.now(),
		})
		o.decPending() // now counted via the pool
		o.log.Info("provisioned", "id", inst.ID, "ip", inst.IPv4)
		o.emit("worker_provisioned", map[string]string{attrID: inst.ID, attrIP: inst.IPv4})

		// Seed the pin before the first dial so WaitReady's handshake is verified.
		if canPin {
			pinner.PinHostKey(inst.IPv4, sshHostPub)
		}

		if err := o.disp.WaitReady(ctx, inst.ID, o.addrForInstance(inst.IPv4, inst.VPCIPv4)); err != nil {
			o.log.Error("worker readiness", "id", inst.ID, "err", err)
			o.finishStoredPhase(ctx, phase.ID, "failed", "readiness", o.now())
			if o.store != nil && generation.ID != 0 {
				_ = o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now())
			}
			o.pool.SetState(inst.ID, StateDraining)
			return
		}
		readyAt := o.now()
		current, exists := o.pool.Get(inst.ID)
		if exists {
			o.recordIntervalCost(ctx, current, storage.CostBoot, current.CreatedAt, readyAt)
			o.finishStoredPhase(ctx, current.PhaseID, "ready", "", readyAt)
		}
		if o.store != nil && generation.ID != 0 {
			if err := o.store.SetGenerationState(ctx, generation.ID, storage.GenerationReady, readyAt); err != nil {
				o.storageError("ready generation", err)
				o.pool.SetState(inst.ID, StateDraining)
				return
			}
		}
		if exists {
			phaseID := o.startStoredPhase(ctx, current, storage.PhaseReadyIdle, 0, readyAt)
			o.pool.SetPhase(inst.ID, phaseID)
		}
		if !o.pool.SetState(inst.ID, StateIdle) {
			return
		}
		o.pool.Touch(inst.ID, readyAt)
		o.log.Info("worker ready", "id", inst.ID)
		o.emit("worker_ready", map[string]string{attrID: inst.ID, attrIP: inst.IPv4})
	})
}

// applyTeardown destroys idle nodes the billing policy says are due. Returns
// the count of Destroy actions kicked off this tick (still in-flight when
// applyTeardown returns; they run on background goroutines).
func (o *Orchestrator) applyTeardown(ctx context.Context) int {
	now := o.now()
	runtime := o.runtimeConfig()
	reaped := 0
	// Draining nodes are dirty strict one-job workers whose immediate delete
	// failed. Retry them independent of normal billing eligibility; they must
	// never become dispatchable again.
	for _, n := range o.pool.ByState(StateDraining) {
		if !o.pool.SetState(n.InstanceID, StateRemoving) {
			continue
		}
		node := n
		reaped++
		o.wg.Go(func() {
			operationID, err := o.beginStoredMutation(ctx, "destroy", node)
			if err != nil {
				o.pool.SetState(node.InstanceID, StateDraining)
				return
			}
			o.finishStoredPhase(ctx, node.PhaseID, "removing", "", o.now())
			node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseRemoving, 0, o.now())
			o.pool.SetPhase(node.InstanceID, node.PhaseID)
			if err := o.prov.Destroy(ctx, node.InstanceID); err != nil {
				o.finishStoredPhase(ctx, node.PhaseID, "failed", err.Error(), o.now())
				o.finishStoredMutation(ctx, operationID, node.InstanceID, "provider_destroy", false)
				o.log.Error("retry dirty worker destroy", "id", node.InstanceID, "err", err)
				o.pool.SetState(node.InstanceID, StateDraining)
				return
			}
			o.finishStoredPhase(ctx, node.PhaseID, "destroyed", "", o.now())
			o.finishStoredMutation(ctx, operationID, node.InstanceID, "", true)
			o.closeStoredResource(ctx, node, storage.ResourceClosed, "dirty_retry")
			o.pool.Delete(node.InstanceID)
			o.emit("worker_reaped", map[string]string{attrID: node.InstanceID, attrIP: node.IP, attrReason: "dirty_retry"})
		})
	}
	idleNodes := o.pool.ByState(StateIdle)
	sort.Slice(idleNodes, func(i, j int) bool {
		if idleNodes[i].CreatedAt.Equal(idleNodes[j].CreatedAt) {
			return idleNodes[i].InstanceID < idleNodes[j].InstanceID
		}
		return idleNodes[i].CreatedAt.After(idleNodes[j].CreatedAt)
	})
	protected := min(runtime.WarmInstances, len(idleNodes))
	for _, n := range idleNodes[protected:] {
		if !runtime.Teardown.ShouldTeardown(n, now) {
			continue
		}
		if !o.pool.SetState(n.InstanceID, StateRemoving) {
			continue
		}
		node := n
		reaped++
		o.wg.Go(func() {
			operationID, err := o.beginStoredMutation(ctx, "destroy", node)
			if err != nil {
				o.pool.SetState(node.InstanceID, StateIdle)
				return
			}
			o.finishStoredPhase(ctx, node.PhaseID, "removing", "", o.now())
			node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseRemoving, 0, o.now())
			o.pool.SetPhase(node.InstanceID, node.PhaseID)
			if err := o.prov.Destroy(ctx, node.InstanceID); err != nil {
				o.finishStoredPhase(ctx, node.PhaseID, "failed", err.Error(), o.now())
				o.finishStoredMutation(ctx, operationID, node.InstanceID, "provider_destroy", false)
				o.log.Error("destroy", "id", node.InstanceID, "err", err)
				if o.store != nil && node.ResourceID != 0 {
					if stateErr := o.store.SetResourceState(ctx, node.ResourceID, storage.ResourceActive, o.now()); stateErr != nil {
						o.storageError("restore idle resource", stateErr)
					}
				}
				node.PhaseID = o.startStoredPhase(ctx, node, storage.PhaseReadyIdle, 0, o.now())
				o.pool.SetPhase(node.InstanceID, node.PhaseID)
				o.pool.SetState(node.InstanceID, StateIdle) // retry next tick
				return
			}
			destroyedAt := o.now()
			o.recordIntervalCost(ctx, node, storage.CostWarmIdle, node.LastBusy, destroyedAt)
			o.finishStoredPhase(ctx, node.PhaseID, "destroyed", "", destroyedAt)
			o.finishStoredMutation(ctx, operationID, node.InstanceID, "", true)
			o.closeStoredResource(ctx, node, storage.ResourceClosed, "billing_policy")
			o.pool.Delete(node.InstanceID)
			o.log.Info("destroyed idle node", "id", node.InstanceID)
			o.emit("worker_reaped", map[string]string{attrID: node.InstanceID, attrIP: node.IP})
		})
	}
	return reaped
}

func phaseKindForState(state NodeState) storage.PhaseKind {
	switch state {
	case StateBusy:
		return storage.PhaseJob
	case StateResetting:
		return storage.PhaseReset
	case StateRemoving, StateDraining:
		return storage.PhaseRemoving
	case StateProvisioning:
		return storage.PhaseProvisioning
	case StateIdle:
		return storage.PhaseReadyIdle
	default:
		return storage.PhaseReadyIdle
	}
}

func (o *Orchestrator) incPending() {
	o.mu.Lock()
	o.pending++
	o.mu.Unlock()
}

func (o *Orchestrator) decPending() {
	o.mu.Lock()
	if o.pending > 0 {
		o.pending--
	}
	o.mu.Unlock()
}

func (o *Orchestrator) pendingCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending
}

func (o *Orchestrator) incBuilders() {
	o.mu.Lock()
	o.builders++
	o.mu.Unlock()
}

func (o *Orchestrator) decBuilders() {
	o.mu.Lock()
	if o.builders > 0 {
		o.builders--
	}
	o.mu.Unlock()
}

func (o *Orchestrator) builderCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.builders
}

// SetManagedImage atomically activates the clean image used for subsequent
// provisions and snapshot resets. Empty returns the tier to cold behavior.
func (o *Orchestrator) SetManagedImage(id string) {
	o.mu.Lock()
	o.managedImage = id
	o.mu.Unlock()
}

func (o *Orchestrator) managedImageID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.managedImage
}

func (o *Orchestrator) addActive(uuid string) {
	o.mu.Lock()
	o.active[uuid] = struct{}{}
	o.mu.Unlock()
}

func (o *Orchestrator) removeActive(uuid string) {
	o.mu.Lock()
	delete(o.active, uuid)
	o.mu.Unlock()
}

func (o *Orchestrator) isActive(uuid string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.active[uuid]
	return ok
}

func (o *Orchestrator) isDispatching(handle string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.dispatching[handle]
	return ok
}

func (o *Orchestrator) markDispatching(handle string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.dispatching[handle]; ok {
		return false
	}
	o.dispatching[handle] = struct{}{}
	return true
}

func (o *Orchestrator) unmarkDispatching(handle string) {
	o.mu.Lock()
	delete(o.dispatching, handle)
	o.mu.Unlock()
}

// filterServiceable keeps jobs whose required labels are all offered by pool.
// The pool's labels may carry a `:scheme://image` binding (see #39); strip it
// before comparing so the binding doesn't make matching fail.
func filterServiceable(jobs []forgejo.WaitingJob, labels []string) []forgejo.WaitingJob {
	have := map[string]struct{}{}
	for _, l := range forgejo.BareLabels(labels) {
		have[l] = struct{}{}
	}
	var out []forgejo.WaitingJob
	for _, j := range jobs {
		ok := true
		for _, want := range j.Labels {
			if _, has := have[want]; !has {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, j)
		}
	}
	return out
}

// generateHostKey mints a fresh ed25519 SSH host keypair for a worker VM. It
// returns the private key as an OpenSSH-format PEM (for cloud-init injection)
// and the matching ssh.PublicKey (for pinning). The keypair is ephemeral per
// VM; the PEM is never logged.
func generateHostKey() (privPEM string, pub ssh.PublicKey, err error) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, fmt.Errorf("generate ed25519 host key: %w", err)
	}
	block, err := ssh.MarshalPrivateKey(privKey, "")
	if err != nil {
		return "", nil, fmt.Errorf("marshal host private key: %w", err)
	}
	sshPub, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return "", nil, fmt.Errorf("derive host public key: %w", err)
	}
	return string(pem.EncodeToMemory(block)), sshPub, nil
}

func shortID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
