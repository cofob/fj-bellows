package router

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

const (
	testAutoLabel          = "ci-auto-amd64"
	testBuildJob           = "build"
	testCloudProvider      = "cloud"
	testDigitalOceanDriver = "digitalocean"
	testDOProvider         = "do"
	testEUR                = "EUR"
	testExplicitLabel      = "ci-short-amd64"
	testFXRate             = "1.1"
	testHetznerType        = "cx33"
	testHetznerProvider    = "hetzner"
	testHZProvider         = "hz"
	testLongTier           = "long"
	testRouteName          = "amd64"
	testPriceSource        = "test"
	testSource             = "forgejo:test"
	testShortTier          = "short"
	testShortType          = "small"
	testUSD                = "USD"
	testUnitFX             = "1"
	testWorkflow           = "ci.yml"
)

type fakeFleet struct {
	snapshots map[string]orchestrator.RoutingTierSnapshot
}

func (f *fakeFleet) RoutingSnapshots(context.Context) map[string]orchestrator.RoutingTierSnapshot {
	out := make(map[string]orchestrator.RoutingTierSnapshot, len(f.snapshots))
	maps.Copy(out, f.snapshots)
	return out
}

type fakeQueue struct {
	jobs []forgejo.WaitingJob
	err  error
}

func (q *fakeQueue) WaitingJobs(context.Context) ([]forgejo.WaitingJob, error) {
	return append([]forgejo.WaitingJob(nil), q.jobs...), q.err
}

type fakeProvider struct {
	quote provider.PriceQuote
	err   error
}

func (*fakeProvider) Configure(context.Context, string, yaml.Node) error { return nil }
func (*fakeProvider) Provision(context.Context, provider.Spec) (provider.Instance, error) {
	return provider.Instance{}, nil
}
func (*fakeProvider) Destroy(context.Context, string) error                     { return nil }
func (*fakeProvider) List(context.Context, string) ([]provider.Instance, error) { return nil, nil }
func (*fakeProvider) BillingModel() provider.BillingModel                       { return provider.BillingPerSecond }
func (p *fakeProvider) Quote(context.Context, string) (provider.PriceQuote, error) {
	return p.quote, p.err
}

func openRouterStore(t *testing.T) *storage.SQLite {
	t.Helper()
	database, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "routing.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func newCostTestRouter(
	t *testing.T,
	database storage.RoutingStore,
	now time.Time,
	queue JobQueue,
	fleet Fleet,
) *Router {
	t.Helper()
	r, err := New(Config{
		Source: testSource, Currency: testUSD,
		ExchangeRates: map[string]string{testUSD: testUnitFX, testEUR: testFXRate},
		PollInterval:  time.Minute, Routes: []Route{{
			Name: testRouteName, RequiredLabel: testAutoLabel,
			Candidates: []string{testShortTier, testLongTier}, FallbackTier: testShortTier,
			HistoryWindow: 720 * time.Hour, MinSamples: 10,
			ColdStartP95: 15 * time.Minute, Queue: queue,
		}},
	}, fleet, map[string]provider.Provider{
		testDOProvider: &fakeProvider{quote: provider.PriceQuote{
			InstanceType: testShortType, Currency: testUSD, PerHourNanos: 2_000_000_000,
			Source: testPriceSource, ObservedAt: now,
		}},
		testHZProvider: &fakeProvider{quote: provider.PriceQuote{
			InstanceType: testHetznerType, Currency: testEUR, PerHourNanos: 1_000_000_000,
			BillingQuantum: time.Hour, MinimumDuration: time.Hour,
			Source: testPriceSource, ObservedAt: now,
		}},
	}, database, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	return r
}

func seedRuntimeHistory(t *testing.T, database storage.RoutingStore, now time.Time) {
	t.Helper()
	for i := range 10 {
		start := now.Add(-time.Duration(i+2) * time.Hour)
		_, err := database.UpsertJob(t.Context(), storage.Job{
			Source: testSource, Handle: "history-" + time.Duration(i).String(),
			RepositoryID: 7, WorkflowFile: testWorkflow, JobName: testBuildJob, Tier: testShortTier,
			Status: storage.JobSucceeded, FirstSeenAt: start, RunnerStartedAt: start,
			RunnerFinishedAt: start.Add(20 * time.Minute), CompletedAt: start.Add(20 * time.Minute),
			UpdatedAt: start.Add(20 * time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRouterUsesColdFallbackAndReplaysAssignment(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 101, Handle: "auto-1", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob,
		Labels: []string{testAutoLabel},
	}}}
	fleet := &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, true),
		testLongTier:  routingSnapshot(testLongTier, testHZProvider, testHetznerProvider, testHetznerType, true),
	}}
	r := newCostTestRouter(t, database, now, queue, fleet)
	r.reconcile(t.Context())
	pending, err := database.PendingRoutedJobs(t.Context(), testShortTier, now.Add(time.Minute))
	if err != nil || len(pending) != 1 {
		t.Fatalf("fallback pending jobs = %+v, %v", pending, err)
	}
	source := &RoutedSource{Base: &emptyJobSource{}, Store: database, Tier: testShortTier, Now: func() time.Time { return now.Add(time.Minute) }}
	jobs, err := source.WaitingJobs(t.Context())
	if err != nil || len(jobs) != 1 || jobs[0].Handle != "auto-1" {
		t.Fatalf("replayed jobs = %+v, %v", jobs, err)
	}
}

func TestRouterChoosesAlreadyPaidWorkerWithHistory(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	seedRuntimeHistory(t, database, now)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 102, Handle: "auto-history", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob,
		Labels: []string{testAutoLabel},
	}}}
	long := routingSnapshot(testLongTier, testHZProvider, testHetznerProvider, testHetznerType, false)
	long.ActiveWorkers, long.AvailableSlots = 1, 0
	long.IdleWorkers = []orchestrator.WorkerView{{
		CreatedAt: now.Add(-5 * time.Minute), ReapEligibleAt: now.Add(50 * time.Minute),
	}}
	fleet := &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, true),
		testLongTier:  long,
	}}
	r := newCostTestRouter(t, database, now, queue, fleet)
	r.reconcile(t.Context())
	pending, err := database.PendingRoutedJobs(t.Context(), testLongTier, now.Add(time.Minute))
	if err != nil || len(pending) != 1 {
		t.Fatalf("paid-window pending jobs = %+v, %v", pending, err)
	}
}

func TestRouterChoosesDigitalOceanWhenPredictedMarginalCostIsLower(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	seedRuntimeHistory(t, database, now)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 103, Handle: "auto-cheapest", RepoID: 7, WorkflowID: testWorkflow,
		Name: testBuildJob, Labels: []string{testAutoLabel},
	}}}
	fleet := &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, true),
		testLongTier:  routingSnapshot(testLongTier, testHZProvider, testHetznerProvider, testHetznerType, true),
	}}
	r := newCostTestRouter(t, database, now, queue, fleet)
	r.reconcile(t.Context())
	pending, err := database.PendingRoutedJobs(t.Context(), testShortTier, now.Add(time.Minute))
	if err != nil || len(pending) != 1 || pending[0].JobID == 0 {
		t.Fatalf("lower-cost DigitalOcean assignment = %+v, %v", pending, err)
	}
}

//nolint:gocyclo // The scenario verifies three arrivals, exact holds, restart replay, and worker loss together.
func TestRouterBoundsPaidWindowWaitQueueBeforeProvisioning(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 110, Handle: "paid-first", RepoID: 7, WorkflowID: testWorkflow,
		Name: testBuildJob, Labels: []string{testAutoLabel},
	}}}
	short := routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, false)
	short.Healthy = false
	worker := orchestrator.WorkerView{
		InstanceID: "hz-paid", State: string(orchestrator.StateIdle),
		CreatedAt: now.Add(-5 * time.Minute), ReapEligibleAt: now.Add(50 * time.Minute),
		BillingModel: "hourly_round_up",
	}
	long := routingSnapshot(testLongTier, testHZProvider, testHetznerProvider, testHetznerType, true)
	long.OneJobPerVM, long.ResetMode = true, resetSnapshot
	long.ActiveWorkers, long.AvailableSlots = 1, 2
	long.IdleWorkers, long.Workers = []orchestrator.WorkerView{worker}, []orchestrator.WorkerView{worker}
	long.Teardown = orchestrator.TeardownPolicy{Model: provider.BillingHourlyRoundUp}
	r := newCostTestRouter(t, database, now, queue, &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: short, testLongTier: long,
	}})
	r.cfg.Routes[0].MaxOptimizationWaitQueue = 1

	// Reconcile each arrival separately. This verifies that the second decision
	// is reconstructed from SQLite rather than relying on one poll batch.
	r.reconcile(t.Context())
	queue.jobs = append(queue.jobs, forgejo.WaitingJob{
		ID: 111, Handle: "paid-wait", RepoID: 7, WorkflowID: testWorkflow,
		Name: testBuildJob, Labels: []string{testAutoLabel},
	})
	r.reconcile(t.Context())
	queue.jobs = append(queue.jobs, forgejo.WaitingJob{
		ID: 112, Handle: "paid-overflow", RepoID: 7, WorkflowID: testWorkflow,
		Name: testBuildJob, Labels: []string{testAutoLabel},
	})
	r.reconcile(t.Context())

	pending, err := database.PendingRoutedJobs(t.Context(), testLongTier, now.Add(time.Minute))
	if err != nil || len(pending) != 3 {
		t.Fatalf("paid-window assignments = %+v, %v", pending, err)
	}
	queued := 0
	for _, job := range pending {
		if job.OptimizationQueued {
			queued++
		}
	}
	if queued != 1 || !pending[1].OptimizationQueued || pending[2].OptimizationQueued {
		t.Fatalf("optimization queue assignments = %+v, want only middle job held", pending)
	}
	source := &RoutedSource{
		Base: &emptyJobSource{}, Store: database, Tier: testLongTier,
		Now: func() time.Time { return now.Add(time.Minute) },
	}
	if _, err := source.WaitingJobs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !source.HoldForRoutingOptimization("paid-wait") ||
		source.HoldForRoutingOptimization("paid-first") ||
		source.HoldForRoutingOptimization("paid-overflow") {
		t.Fatal("routed source did not expose the exact optimization-held handle")
	}
	reservations, err := database.RoutingReservations(t.Context(), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(reservations) != 2 || reservations[1].OptimizationQueuePosition != 1 {
		t.Fatalf("paid worker reservations = %+v", reservations)
	}

	// A disappeared worker releases only the provisioning hold; the durable
	// tier assignment and immutable scorecard remain intact.
	fleet, ok := r.fleet.(*fakeFleet)
	if !ok {
		t.Fatal("router fleet does not use the expected fake")
	}
	long.IdleWorkers, long.Workers = nil, nil
	fleet.snapshots[testLongTier] = long
	r.reconcile(t.Context())
	pending, err = database.PendingRoutedJobs(t.Context(), testLongTier, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range pending {
		if job.OptimizationQueued {
			t.Fatalf("vanished worker left optimization hold active: %+v", pending)
		}
	}
}

func TestRouterReservesCapacityAcrossJobs(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{
		{ID: 201, Handle: "auto-201", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob, Labels: []string{testAutoLabel}},
		{ID: 202, Handle: "auto-202", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob, Labels: []string{testAutoLabel}},
	}}
	short := routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, true)
	long := routingSnapshot(testLongTier, testHZProvider, testHetznerProvider, testHetznerType, true)
	short.AvailableSlots, short.MaxInstances = 1, 1
	long.AvailableSlots, long.MaxInstances = 1, 1
	r := newCostTestRouter(t, database, now, queue, &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: short, testLongTier: long,
	}})
	r.reconcile(t.Context())
	for _, tier := range []string{testShortTier, testLongTier} {
		pending, pendingErr := database.PendingRoutedJobs(t.Context(), tier, now.Add(time.Minute))
		if pendingErr != nil || len(pending) != 1 {
			t.Fatalf("%s pending jobs = %+v, %v", tier, pending, pendingErr)
		}
	}
}

func TestRouterGivesExplicitTierLabelPrecedence(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 301, Handle: "explicit", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob,
		Labels: []string{testAutoLabel, testExplicitLabel},
	}}}
	r, err := New(Config{
		Source: testSource, Currency: testUSD, ExchangeRates: map[string]string{testUSD: testUnitFX},
		PollInterval: time.Minute, ExplicitLabels: []string{testExplicitLabel}, Routes: []Route{{
			Name: testRouteName, RequiredLabel: testAutoLabel, Candidates: []string{testShortTier},
			FallbackTier: testShortTier, HistoryWindow: 720 * time.Hour, MinSamples: 10,
			ColdStartP95: 15 * time.Minute, Queue: queue,
		}},
	}, &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, true),
	}}, map[string]provider.Provider{
		testDOProvider: &fakeProvider{quote: provider.PriceQuote{InstanceType: testShortType, Currency: testUSD, PerHourNanos: 1, Source: testPriceSource, ObservedAt: now}},
	}, database, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	r.reconcile(t.Context())
	jobs, err := database.OpenJobs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("explicitly routed job was observed by auto-router: %+v", jobs)
	}
}

func TestRoutedSourceHidesUnassignedAutomaticJobs(t *testing.T) {
	database := openRouterStore(t)
	source := &RoutedSource{
		Base: &emptyJobSource{jobs: []forgejo.WaitingJob{
			{ID: 1, Handle: "auto-only", Labels: []string{testAutoLabel}},
			{ID: 2, Handle: "explicit", Labels: []string{testAutoLabel, testExplicitLabel}},
		}},
		Store: database, Tier: testShortTier, AutomaticLabels: []string{testAutoLabel},
		ExplicitLabels: []string{testExplicitLabel},
	}
	jobs, err := source.WaitingJobs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Handle != "explicit" {
		t.Fatalf("WaitingJobs = %+v, want only explicit job", jobs)
	}
}

func TestRouterDefersWhenEveryCandidateIsFull(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 401, Handle: "deferred", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob,
		Labels: []string{testAutoLabel},
	}}}
	short := routingSnapshot(testShortTier, testDOProvider, testDigitalOceanDriver, testShortType, false)
	long := routingSnapshot(testLongTier, testHZProvider, testHetznerProvider, testHetznerType, false)
	r, err := New(Config{
		Source: testSource, Currency: testUSD, ExchangeRates: map[string]string{testUSD: testUnitFX, testEUR: testFXRate},
		PollInterval: time.Minute, Routes: []Route{{
			Name: testRouteName, RequiredLabel: testAutoLabel, Candidates: []string{testShortTier, testLongTier},
			FallbackTier: testShortTier, HistoryWindow: 720 * time.Hour, MinSamples: 10,
			ColdStartP95: 15 * time.Minute, Queue: queue,
		}},
	}, &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{testShortTier: short, testLongTier: long}},
		map[string]provider.Provider{
			testDOProvider: &fakeProvider{quote: provider.PriceQuote{InstanceType: testShortType, Currency: testUSD, PerHourNanos: 1, Source: testPriceSource, ObservedAt: now}},
			testHZProvider: &fakeProvider{quote: provider.PriceQuote{InstanceType: testHetznerType, Currency: testEUR, PerHourNanos: 1, Source: testPriceSource, ObservedAt: now}},
		}, database, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	r.reconcile(t.Context())
	jobs, err := database.OpenJobs(t.Context())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("OpenJobs = %+v, %v", jobs, err)
	}
	decision, err := database.ObserveRoutingDecision(t.Context(), storage.RoutingDecision{
		JobID: jobs[0].ID, Route: testRouteName, RequiredLabel: testAutoLabel,
		PayloadJSON: `{}`, FallbackTier: testShortTier, FirstSeenAt: now, PolicyVersion: policyVersion,
	})
	if err != nil || decision.State != storage.RoutingPending || decision.DeferCount != 1 {
		t.Fatalf("deferred decision = %+v, %v", decision, err)
	}
}

func TestRouterUsesStoredQuoteAndReportsDegradedPricing(t *testing.T) {
	database := openRouterStore(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	for _, instanceType := range []string{testShortType, "large"} {
		if _, err := database.RecordPriceQuote(t.Context(), storage.PriceQuote{
			Provider: testCloudProvider, Driver: testPriceSource, InstanceType: instanceType,
			Currency: testUSD, PerHourNanos: 100, Source: "stored", ObservedAt: now.Add(-24 * time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	queue := &fakeQueue{jobs: []forgejo.WaitingJob{{
		ID: 501, Handle: "cached-price", RepoID: 7, WorkflowID: testWorkflow, Name: testBuildJob,
		Labels: []string{testAutoLabel},
	}}}
	r, err := New(Config{
		Source: testSource, Currency: testUSD, ExchangeRates: map[string]string{testUSD: testUnitFX},
		PollInterval: time.Minute, Routes: []Route{{
			Name: testRouteName, RequiredLabel: testAutoLabel, Candidates: []string{testShortTier, testLongTier},
			FallbackTier: testShortTier, HistoryWindow: 720 * time.Hour, MinSamples: 10,
			ColdStartP95: 15 * time.Minute, Queue: queue,
		}},
	}, &fakeFleet{snapshots: map[string]orchestrator.RoutingTierSnapshot{
		testShortTier: routingSnapshot(testShortTier, testCloudProvider, testPriceSource, testShortType, true),
		testLongTier:  routingSnapshot(testLongTier, testCloudProvider, testPriceSource, "large", true),
	}}, map[string]provider.Provider{testCloudProvider: &fakeProvider{err: errors.New("catalog unavailable")}}, database, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	r.reconcile(t.Context())
	health := r.Health()
	if !health.Healthy || !health.DegradedPricing {
		t.Fatalf("Health = %+v, want healthy with degraded pricing", health)
	}
	pending, err := database.PendingRoutedJobs(t.Context(), testShortTier, now.Add(time.Minute))
	if err != nil || len(pending) != 1 {
		t.Fatalf("stored-price assignment = %+v, %v", pending, err)
	}
}

func routingSnapshot(tier, providerName, driver, instanceType string, cold bool) orchestrator.RoutingTierSnapshot {
	snapshot := orchestrator.RoutingTierSnapshot{
		Tier: tier, ProviderName: providerName, Driver: driver, InstanceType: instanceType,
		Labels: []string{testAutoLabel}, Healthy: true, MaxInstances: 10,
	}
	if cold {
		snapshot.AvailableSlots = 10
	}
	return snapshot
}

type emptyJobSource struct{ jobs []forgejo.WaitingJob }

func (s *emptyJobSource) WaitingJobs(context.Context) ([]forgejo.WaitingJob, error) {
	return append([]forgejo.WaitingJob(nil), s.jobs...), nil
}

func (*emptyJobSource) RegisterEphemeral(context.Context, string, []string) (forgejo.Registration, error) {
	return forgejo.Registration{}, nil
}
func (*emptyJobSource) ListRunners(context.Context) ([]forgejo.Runner, error) { return nil, nil }
func (*emptyJobSource) DeleteRunner(context.Context, int64) error             { return nil }
