// Package router implements fleet-level, cost-aware routing between ordinary
// runner tiers. Provider adapters remain unaware of job routing.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/hstern/fj-bellows/internal/control/events"
	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

const (
	policyVersion       = "cost-p95-v2-paid-window-queue"
	quoteRefresh        = time.Hour
	profileColdFallback = "cold_fallback"
	profileGlobalP95    = "global_p95"
	profileTierP95      = "tier_p95"
	idleValue           = "idle"
)

// JobQueue is the read-only queue operation used by one automatic route.
type JobQueue interface {
	WaitingJobs(ctx context.Context) ([]forgejo.WaitingJob, error)
}

// Fleet exposes candidate capacity without allowing the router to mutate it.
type Fleet interface {
	RoutingSnapshots(ctx context.Context) map[string]orchestrator.RoutingTierSnapshot
}

// Route configures one automatic label.
type Route struct {
	Name                     string
	RequiredLabel            string
	Candidates               []string
	FallbackTier             string
	HistoryWindow            time.Duration
	MinSamples               int
	ColdStartP95             time.Duration
	MaxOptimizationWaitQueue int
	Queue                    JobQueue
}

// Config is the router's fully resolved runtime configuration.
type Config struct {
	Source         string
	Currency       string
	ExchangeRates  map[string]string
	PollInterval   time.Duration
	ExplicitLabels []string
	Routes         []Route
}

// Health reports whether automatic queue polls and price refreshes are live.
type Health struct {
	Healthy         bool
	LastPollAt      time.Time
	LastDecisionAt  time.Time
	LastError       string
	DegradedPricing bool
}

type cachedQuote struct {
	quote     storage.PriceQuote
	refreshed time.Time
	degraded  bool
}

// Router serially evaluates every route so capacity reservations are shared
// across automatic labels within a process.
type Router struct {
	cfg       Config
	fleet     Fleet
	providers map[string]provider.Provider
	store     storage.RoutingStore
	log       *slog.Logger
	events    *events.Bus
	now       func() time.Time

	mu     sync.Mutex
	health Health
	quotes map[string]cachedQuote
}

// New constructs a router. Provider pricing capability is checked lazily
// against resolved tier snapshots so ordinary non-routed providers are free
// to omit Pricer.
func New(cfg Config, fleet Fleet, providers map[string]provider.Provider, store storage.RoutingStore, log *slog.Logger) (*Router, error) {
	if fleet == nil || store == nil || len(cfg.Routes) == 0 {
		return nil, errors.New("router: fleet, store, and routes are required")
	}
	if cfg.PollInterval <= 0 {
		return nil, errors.New("router: poll interval must be positive")
	}
	if log == nil {
		log = slog.Default()
	}
	routes := append([]Route(nil), cfg.Routes...)
	sort.Slice(routes, func(i, j int) bool { return routes[i].Name < routes[j].Name })
	cfg.Routes = routes
	return &Router{
		cfg: cfg, fleet: fleet, providers: providers, store: store, log: log,
		events: events.New(), now: time.Now, quotes: map[string]cachedQuote{},
	}, nil
}

// Run polls automatic queues until cancellation. Upstream failures degrade
// health and are retried instead of terminating the worker fleet.
func (r *Router) Run(ctx context.Context) error {
	r.reconcile(ctx)
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// Subscribe returns automatic routing events.
func (r *Router) Subscribe() (<-chan events.Event, func()) { return r.events.Subscribe() }

// Health returns the router's current freshness/degradation state.
func (r *Router) Health() Health {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := r.health
	result.Healthy = result.LastError == "" && !result.LastPollAt.IsZero() &&
		r.now().Sub(result.LastPollAt) <= 3*r.cfg.PollInterval
	return result
}

func (r *Router) reconcile(ctx context.Context) {
	now := r.now().UTC()
	r.setPricingDegraded(false)
	snapshots := r.fleet.RoutingSnapshots(ctx)
	phaseProfiles := map[string]storage.WorkflowProfile{}
	var failures []error
	scheduling, err := loadSchedulingState(ctx, r.store, snapshots, now)
	if err != nil {
		failures = append(failures, fmt.Errorf("restore optimization queue: %w", err))
		scheduling = &schedulingState{lanes: map[string][]optimizationLane{}, queuedByRoute: map[string]int{}}
	}
	for _, route := range r.cfg.Routes {
		jobs, err := route.Queue.WaitingJobs(ctx)
		if err != nil {
			if preserveErr := r.store.PreserveRoutingAssignments(ctx, route.Name, now, now.Add(3*r.cfg.PollInterval)); preserveErr != nil {
				err = errors.Join(err, fmt.Errorf("preserve assignments: %w", preserveErr))
			}
			failures = append(failures, fmt.Errorf("route %s queue: %w", route.Name, err))
			continue
		}
		sort.SliceStable(jobs, func(i, j int) bool {
			if jobs[i].ID == jobs[j].ID {
				return jobs[i].Handle < jobs[j].Handle
			}
			return jobs[i].ID < jobs[j].ID
		})
		for i := range jobs {
			if err := r.routeJob(ctx, route, &jobs[i], snapshots, scheduling, phaseProfiles, now); err != nil {
				failures = append(failures, fmt.Errorf("route %s job %s: %w", route.Name, jobs[i].Handle, err))
			}
		}
	}
	r.mu.Lock()
	r.health.LastPollAt = now
	if len(failures) == 0 {
		r.health.LastError = ""
	} else {
		r.health.LastError = errors.Join(failures...).Error()
	}
	r.mu.Unlock()
	for _, err := range failures {
		r.log.Error("automatic routing", "err", err)
	}
}

//nolint:gocyclo,funlen // One job decision deliberately keeps persistence, profiling, scoring, and reservation ordering visible.
func (r *Router) routeJob(
	ctx context.Context,
	route Route,
	job *forgejo.WaitingJob,
	snapshots map[string]orchestrator.RoutingTierSnapshot,
	scheduling *schedulingState,
	phaseProfiles map[string]storage.WorkflowProfile,
	now time.Time,
) error {
	if hasExplicitLabel(job.Labels, r.cfg.ExplicitLabels) {
		return nil
	}
	if job.Handle == "" {
		job.Handle = strconv.FormatInt(job.ID, 10)
	}
	payload, err := json.Marshal(job)
	if err != nil {
		return err
	}
	workflow := job.WorkflowID
	quality := storage.IdentityExact
	if workflow == "" {
		workflow, quality = job.Name, storage.IdentityFallback
	}
	storedJob, err := r.store.UpsertJob(ctx, storage.Job{
		Source: r.cfg.Source, Handle: job.Handle, ForgejoJobID: job.ID, Attempt: job.Attempt,
		RepositoryID: job.RepoID, RepositoryOwnerID: job.OwnerID, WorkflowFile: workflow,
		JobName: job.Name, IdentityQuality: quality, Status: storage.JobObserved,
		QueueMeasurementSource: "auto_router", FirstSeenAt: now, QueuedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	decision, err := r.store.ObserveRoutingDecision(ctx, storage.RoutingDecision{
		JobID: storedJob.ID, Route: route.Name, RequiredLabel: route.RequiredLabel,
		PayloadJSON: string(payload), FallbackTier: route.FallbackTier,
		FirstSeenAt: now, PolicyVersion: policyVersion,
	})
	if err != nil {
		return err
	}
	expires := now.Add(3 * r.cfg.PollInterval)
	if decision.State == storage.RoutingAssigned {
		return r.store.RenewRoutingDecision(ctx, storedJob.ID, string(payload), expires)
	}
	historyFrom := now.Add(-route.HistoryWindow)
	globalProfile, err := r.store.WorkflowProfile(ctx, storage.WorkflowProfileFilter{
		Source: r.cfg.Source, RepositoryID: job.RepoID, Workflow: workflow,
		JobName: job.Name, From: historyFrom, To: now,
	})
	if err != nil {
		return err
	}
	hasHistory := globalProfile.Samples >= int64(route.MinSamples)
	scores := make([]scoredCandidate, 0, len(route.Candidates))
	for _, tier := range route.Candidates {
		snapshot, ok := snapshots[tier]
		if !ok {
			scores = append(scores, scoredCandidate{RoutingCandidateScore: storage.RoutingCandidateScore{Tier: tier, Reason: "tier_missing"}})
			continue
		}
		predicted, samples, profileSource := route.ColdStartP95, globalProfile.Samples, profileColdFallback
		if hasHistory {
			predicted, profileSource = globalProfile.P95, profileGlobalP95
			tierProfile, profileErr := r.store.WorkflowProfile(ctx, storage.WorkflowProfileFilter{
				Source: r.cfg.Source, RepositoryID: job.RepoID, Workflow: workflow,
				JobName: job.Name, Tier: tier, From: historyFrom, To: now,
			})
			if profileErr != nil {
				return profileErr
			}
			if tierProfile.Samples >= int64(route.MinSamples) {
				predicted, samples, profileSource = tierProfile.P95, tierProfile.Samples, profileTierP95
			}
		}
		quote, quoteErr := r.quoteFor(ctx, snapshot, now)
		fx := r.cfg.ExchangeRates[quote.Currency]
		boot := r.phaseProfile(ctx, phaseProfiles, tier, storage.PhaseProvisioning, historyFrom, now)
		reset := r.phaseProfile(ctx, phaseProfiles, tier, storage.PhaseReset, historyFrom, now)
		allowQueue := route.MaxOptimizationWaitQueue > scheduling.queuedByRoute[route.Name]
		score := scoreCandidateWithQueue(snapshot, quote, fx, job.Labels, predicted,
			boot.P95, reset.P95, now, scheduling.lanes[tier], allowQueue)
		if quoteErr != nil && score.Reason == "pricing_unavailable" {
			score.Reason = "pricing_unavailable: " + quoteErr.Error()
		}
		score.ProfileSamples = samples
		score.ProfileSource = profileSource
		scores = append(scores, score)
	}
	selected := selectCandidate(scores, route, hasHistory)
	if selected < 0 {
		if err := r.store.DeferRoutingDecision(ctx, storedJob.ID, "no_eligible_capacity", now); err != nil {
			return err
		}
		r.emit("route_deferred", map[string]string{"route": route.Name, "handle": job.Handle})
		return nil
	}
	rankCandidates(scores, route.Candidates)
	for i := range scores {
		scores[i].Selected = i == selected
	}
	fallback := scoreForTier(scores, route.FallbackTier)
	chosen := scores[selected]
	decision.ProfileSource = chosen.ProfileSource
	decision.ProfileSamples = chosen.ProfileSamples
	decision.PredictedP95 = chosen.PredictedRun
	decision.HistoryFrom, decision.HistoryTo = historyFrom, now
	decision.SelectedTier, decision.SelectedProvider = chosen.Tier, chosen.Provider
	decision.SelectedDriver, decision.SelectedIdle = chosen.Driver, chosen.UsedIdle
	decision.SelectionReason = selectionReason(hasHistory, chosen.Tier == route.FallbackTier)
	if chosen.OptimizationQueued {
		decision.SelectionReason = "paid_window_wait_queue"
	}
	decision.ScoreCurrency = r.cfg.Currency
	decision.SelectedCostNanos, decision.SelectedCostKnown = chosen.ScoreCostNanos, chosen.ScoreCostKnown
	if fallback != nil {
		decision.FallbackCostNanos, decision.FallbackCostKnown = fallback.ScoreCostNanos, fallback.ScoreCostKnown
	}
	decision.DecidedAt, decision.ExpiresAt = now, expires
	decision.OptimizationQueued = chosen.OptimizationQueued
	decision.OptimizationActive = chosen.OptimizationQueued
	decision.SelectedWorkerID = chosen.IdleInstanceID
	decision.ScheduledStartAt = chosen.ScheduledStartAt
	decision.ScheduledFinishAt = chosen.ScheduledFinishAt
	decision.OptimizationQueuePosition = chosen.OptimizationQueuePosition
	decision.OptimizationWait = chosen.OptimizationWait
	candidates := make([]storage.RoutingCandidateScore, len(scores))
	for i := range scores {
		candidates[i] = scores[i].RoutingCandidateScore
	}
	if err := r.store.AssignRoutingDecision(ctx, decision, candidates); err != nil {
		return err
	}
	consumeCapacity(snapshots, scheduling, route.Name, chosen)
	r.mu.Lock()
	r.health.LastDecisionAt = now
	r.mu.Unlock()
	r.emit("route_decided", map[string]string{
		"route": route.Name, "handle": job.Handle, "tier": chosen.Tier,
		"provider": chosen.Provider, "predicted_p95": chosen.PredictedRun.String(),
		"score_nanos": strconv.FormatInt(chosen.ScoreCostNanos, 10),
		idleValue:     strconv.FormatBool(chosen.UsedIdle), "reason": decision.SelectionReason,
		"optimization_queued": strconv.FormatBool(chosen.OptimizationQueued),
		"optimization_wait":   chosen.OptimizationWait.String(),
	})
	return nil
}

func (r *Router) phaseProfile(ctx context.Context, cache map[string]storage.WorkflowProfile, tier string, kind storage.PhaseKind, from, to time.Time) storage.WorkflowProfile {
	key := tier + "\x00" + string(kind)
	if profile, ok := cache[key]; ok {
		return profile
	}
	profile, err := r.store.PhaseProfile(ctx, tier, kind, from, to)
	if err != nil {
		r.log.Debug("routing phase profile", "tier", tier, "kind", kind, "err", err)
	}
	cache[key] = profile
	return profile
}

func (r *Router) quoteFor(ctx context.Context, snapshot orchestrator.RoutingTierSnapshot, now time.Time) (storage.PriceQuote, error) {
	key := snapshot.ProviderName + "\x00" + snapshot.InstanceType
	if cached, ok := r.quotes[key]; ok && now.Sub(cached.refreshed) < quoteRefresh {
		if cached.degraded {
			r.setPricingDegraded(true)
		}
		return cached.quote, nil
	}
	prov := r.providers[snapshot.ProviderName]
	pricer, ok := prov.(provider.Pricer)
	if !ok {
		return storage.PriceQuote{}, errors.New("provider does not implement pricing")
	}
	quote, err := pricer.Quote(ctx, snapshot.InstanceType)
	if err == nil {
		stored, storeErr := r.store.RecordPriceQuote(ctx, storage.PriceQuote{
			Provider: snapshot.ProviderName, Driver: snapshot.Driver, InstanceType: quote.InstanceType,
			Currency: quote.Currency, PerHourNanos: quote.PerHourNanos,
			PerMonthNanos: quote.PerMonthNanos, SnapshotGBMonthNanos: quote.SnapshotGBMonthNanos,
			MinimumChargeNanos: quote.MinimumChargeNanos, BillingQuantum: quote.BillingQuantum,
			MinimumDuration: quote.MinimumDuration, Source: quote.Source, ObservedAt: quote.ObservedAt,
		})
		if storeErr == nil {
			r.quotes[key] = cachedQuote{quote: stored, refreshed: now}
			return stored, nil
		}
		err = storeErr
	}
	cached, cacheErr := r.store.LatestPriceQuote(ctx, snapshot.ProviderName, snapshot.InstanceType)
	if cacheErr == nil {
		r.quotes[key] = cachedQuote{quote: cached, refreshed: now, degraded: true}
		r.setPricingDegraded(true)
		return cached, err
	}
	r.setPricingDegraded(true)
	return storage.PriceQuote{}, errors.Join(err, cacheErr)
}

func (r *Router) setPricingDegraded(value bool) {
	r.mu.Lock()
	r.health.DegradedPricing = value
	r.mu.Unlock()
}

func selectCandidate(scores []scoredCandidate, route Route, hasHistory bool) int {
	if !hasHistory {
		for i := range scores {
			if scores[i].Tier == route.FallbackTier && scores[i].Eligible {
				return i
			}
		}
	}
	best := -1
	var bestScore scoredCandidate
	for i := range scores {
		if !scores[i].Eligible || !scores[i].ScoreCostKnown {
			continue
		}
		if best < 0 {
			best = i
			bestScore = scores[i]
			continue
		}
		if candidateLess(scores[i], bestScore, route.Candidates) {
			best = i
			bestScore = scores[i]
		}
	}
	return best
}

func candidateLess(left, right scoredCandidate, order []string) bool {
	if left.ScoreCostNanos != right.ScoreCostNanos {
		return left.ScoreCostNanos < right.ScoreCostNanos
	}
	if left.UsedIdle != right.UsedIdle {
		return left.UsedIdle
	}
	if left.startDelay != right.startDelay {
		return left.startDelay < right.startDelay
	}
	leftIndex, rightIndex := tierIndex(order, left.Tier), tierIndex(order, right.Tier)
	if leftIndex != rightIndex {
		return leftIndex < rightIndex
	}
	return left.Tier < right.Tier
}

func tierIndex(order []string, tier string) int {
	for i, candidate := range order {
		if candidate == tier {
			return i
		}
	}
	return len(order)
}

func rankCandidates(scores []scoredCandidate, order []string) {
	indices := make([]int, 0, len(scores))
	for i := range scores {
		if scores[i].Eligible && scores[i].ScoreCostKnown {
			indices = append(indices, i)
		}
	}
	sort.Slice(indices, func(i, j int) bool {
		return candidateLess(scores[indices[i]], scores[indices[j]], order)
	})
	for rank, index := range indices {
		scores[index].Rank = rank + 1
	}
}

func scoreForTier(scores []scoredCandidate, tier string) *scoredCandidate {
	for i := range scores {
		if scores[i].Tier == tier {
			return &scores[i]
		}
	}
	return nil
}

func consumeCapacity(
	snapshots map[string]orchestrator.RoutingTierSnapshot,
	scheduling *schedulingState,
	route string,
	selected scoredCandidate,
) {
	snapshot := snapshots[selected.Tier]
	switch {
	case selected.UsedIdle && selected.idleIndex >= 0 && selected.idleIndex < len(snapshot.IdleWorkers):
		snapshot.IdleWorkers = append(snapshot.IdleWorkers[:selected.idleIndex], snapshot.IdleWorkers[selected.idleIndex+1:]...)
		if selected.laneIndex >= 0 && selected.laneIndex < len(scheduling.lanes[selected.Tier]) {
			lane := scheduling.lanes[selected.Tier][selected.laneIndex]
			lane.availableAt = selected.ScheduledFinishAt
			scheduling.lanes[selected.Tier][selected.laneIndex] = lane
		}
	case selected.OptimizationQueued && selected.laneIndex >= 0 &&
		selected.laneIndex < len(scheduling.lanes[selected.Tier]):
		lane := scheduling.lanes[selected.Tier][selected.laneIndex]
		lane.availableAt = selected.ScheduledFinishAt
		lane.queued++
		scheduling.lanes[selected.Tier][selected.laneIndex] = lane
		scheduling.queuedByRoute[route]++
	case snapshot.AvailableSlots > 0:
		snapshot.AvailableSlots--
		snapshot.PendingWorkers++
	}
	snapshots[selected.Tier] = snapshot
}

func selectionReason(history, fallback bool) string {
	if !history && fallback {
		return profileColdFallback
	}
	if !history {
		return "cold_fallback_unavailable"
	}
	return "lowest_marginal_cost"
}

func hasExplicitLabel(labels, explicit []string) bool {
	for _, label := range labels {
		if slices.Contains(explicit, label) {
			return true
		}
	}
	return false
}

func (r *Router) emit(kind string, attrs map[string]string) {
	r.events.Publish(events.Event{At: r.now().UTC(), Type: kind, Attrs: attrs})
}
