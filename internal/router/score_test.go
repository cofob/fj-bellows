package router

import (
	"testing"
	"time"

	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

func TestScoreCandidateUsesPaidHourlyWindow(t *testing.T) {
	now := time.Date(2026, time.July, 22, 10, 5, 0, 0, time.UTC)
	quote := storage.PriceQuote{
		ID: 1, Currency: testEUR, PerHourNanos: 1_000_000_000,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
	}
	snapshot := orchestrator.RoutingTierSnapshot{
		Tier: testLongTier, ProviderName: testHetznerProvider, Driver: testHetznerProvider, InstanceType: testHetznerType,
		Labels: []string{testAutoLabel}, Healthy: true, MaxInstances: 10,
		ActiveWorkers: 1, IdleWorkers: []orchestrator.WorkerView{{
			CreatedAt: now.Add(-5 * time.Minute), ReapEligibleAt: now.Add(50 * time.Minute),
		}},
	}
	got := scoreCandidate(snapshot, quote, testFXRate, []string{testAutoLabel},
		20*time.Minute, 0, 0, now)
	if !got.Eligible || !got.UsedIdle || got.NativeCostNanos != 0 || got.ScoreCostNanos != 0 {
		t.Fatalf("paid-window score = %+v", got)
	}

	snapshot.IdleWorkers = nil
	snapshot.ActiveWorkers = 0
	snapshot.AvailableSlots = 10
	got = scoreCandidate(snapshot, quote, testFXRate, []string{testAutoLabel},
		20*time.Minute, 2*time.Minute, 0, now)
	if !got.Eligible || got.UsedIdle || got.NativeCostNanos != 1_000_000_000 ||
		got.ScoreCostNanos != 1_100_000_000 {
		t.Fatalf("cold hourly score = %+v", got)
	}
}

func TestScoreCandidateQueuesBehindPaidWorkerBeforeColdProvision(t *testing.T) {
	now := time.Date(2026, time.July, 22, 10, 5, 0, 0, time.UTC)
	worker := orchestrator.WorkerView{
		InstanceID: "paid-worker", CreatedAt: now.Add(-5 * time.Minute),
		ReapEligibleAt: now.Add(50 * time.Minute), BillingModel: "hourly_round_up",
	}
	snapshot := orchestrator.RoutingTierSnapshot{
		Tier: testLongTier, ProviderName: testHZProvider, Driver: testHetznerProvider,
		InstanceType: testHetznerType, Labels: []string{testAutoLabel}, Healthy: true,
		OneJobPerVM: true, ResetMode: resetSnapshot, MaxInstances: 2, AvailableSlots: 1,
		Teardown: orchestrator.TeardownPolicy{Model: provider.BillingHourlyRoundUp},
	}
	quote := storage.PriceQuote{
		ID: 1, Currency: testEUR, PerHourNanos: 1_000_000_000,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
	}
	lanes := []optimizationLane{{worker: worker, availableAt: now.Add(17 * time.Minute)}}
	got := scoreCandidateWithQueue(snapshot, quote, testUnitFX, []string{testAutoLabel},
		5*time.Minute, 2*time.Minute, 2*time.Minute, now, lanes, true)
	if !got.Eligible || !got.OptimizationQueued || got.IdleInstanceID != worker.InstanceID ||
		got.OptimizationWait != 17*time.Minute || got.OptimizationQueuePosition != 1 ||
		got.NativeCostNanos != 0 || !got.ScheduledFinishAt.Equal(now.Add(24*time.Minute)) {
		t.Fatalf("paid-window queued score = %+v", got)
	}

	got = scoreCandidateWithQueue(snapshot, quote, testUnitFX, []string{testAutoLabel},
		5*time.Minute, 2*time.Minute, 2*time.Minute, now, lanes, false)
	if got.OptimizationQueued || got.UsedIdle || got.NativeCostNanos != 1_000_000_000 {
		t.Fatalf("full optimization queue score = %+v", got)
	}
}

func TestScoreCandidateAppliesPerSecondMinimumAndCapacity(t *testing.T) {
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	quote := storage.PriceQuote{
		ID: 1, Currency: testUSD, PerHourNanos: 360_000_000,
		MinimumChargeNanos: 20_000_000,
	}
	snapshot := orchestrator.RoutingTierSnapshot{
		Tier: testShortTier, ProviderName: testDOProvider, Driver: testDigitalOceanDriver, InstanceType: testShortType,
		Labels: []string{testAutoLabel}, Healthy: true, MaxInstances: 1, AvailableSlots: 1,
	}
	got := scoreCandidate(snapshot, quote, testUnitFX, []string{testAutoLabel},
		30*time.Second, 30*time.Second, 0, now)
	if !got.Eligible || got.NativeCostNanos != 20_000_000 {
		t.Fatalf("per-second minimum score = %+v", got)
	}
	snapshot.AvailableSlots = 0
	got = scoreCandidate(snapshot, quote, testUnitFX, []string{testAutoLabel},
		30*time.Second, 30*time.Second, 0, now)
	if got.Eligible || got.Reason != reasonCapacityFull {
		t.Fatalf("full candidate score = %+v", got)
	}
}

func TestBilledCostKeepsMinimumChargeWithMonthlyCap(t *testing.T) {
	start := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	quote := storage.PriceQuote{
		PerHourNanos: 1, PerMonthNanos: 100, MinimumChargeNanos: 20,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
	}
	if got := billedCost(quote, start, start.Add(time.Minute)); got != 20 {
		t.Fatalf("billedCost = %d, want minimum charge 20", got)
	}
	quote.PerMonthNanos = 10
	if got := billedCost(quote, start, start.Add(time.Minute)); got != 10 {
		t.Fatalf("billedCost = %d, want monthly cap 10", got)
	}
}

func TestBilledCostAppliesMonthlyCapsPerCalendarMonth(t *testing.T) {
	quote := storage.PriceQuote{PerHourNanos: 1_000, PerMonthNanos: 2_000}
	withinMonth := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	if got := billedCost(quote, withinMonth, withinMonth.Add(3*time.Hour)); got != 2_000 {
		t.Fatalf("within-month capped cost = %d, want 2000", got)
	}
	acrossMonth := time.Date(2026, time.July, 31, 23, 0, 0, 0, time.UTC)
	if got := billedCost(quote, acrossMonth, acrossMonth.Add(3*time.Hour)); got != 3_000 {
		t.Fatalf("cross-month capped cost = %d, want 3000", got)
	}
}

func TestFixedPointFXAndCandidateTies(t *testing.T) {
	if got, ok := normalizeNanos(101, "1.08"); !ok || got != 109 {
		t.Fatalf("normalizeNanos = %d, %t; want 109, true", got, ok)
	}
	scores := []scoredCandidate{
		{RoutingCandidateScore: storage.RoutingCandidateScore{Tier: "first", Eligible: true, ScoreCostKnown: true, ScoreCostNanos: 10}},
		{RoutingCandidateScore: storage.RoutingCandidateScore{Tier: idleValue, Eligible: true, UsedIdle: true, ScoreCostKnown: true, ScoreCostNanos: 10}},
	}
	selected := selectCandidate(scores, Route{Candidates: []string{"first", idleValue}}, true)
	if selected != 1 {
		t.Fatalf("selected tie index = %d, want idle candidate", selected)
	}
	scores[1].UsedIdle = false
	selected = selectCandidate(scores, Route{Candidates: []string{"first", idleValue}}, true)
	if selected != 0 {
		t.Fatalf("selected ordered tie index = %d, want first candidate", selected)
	}
}

func TestColdFallbackRemainsEligibleWithoutAQuote(t *testing.T) {
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	score := scoreCandidate(orchestrator.RoutingTierSnapshot{
		Tier: "fallback", Labels: []string{testAutoLabel}, Healthy: true,
		MaxInstances: 1, AvailableSlots: 1,
	}, storage.PriceQuote{}, "", []string{testAutoLabel}, 15*time.Minute, 0, 0, now)
	if !score.Eligible || score.ScoreCostKnown || score.Reason != "pricing_unavailable" {
		t.Fatalf("unpriced fallback score = %+v", score)
	}
	if selected := selectCandidate([]scoredCandidate{score}, Route{FallbackTier: "fallback"}, false); selected != 0 {
		t.Fatalf("cold fallback selection = %d, want 0", selected)
	}
}

func TestConsumeCapacityRemovesTheIdleWorkerThatWasScored(t *testing.T) {
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	snapshot := orchestrator.RoutingTierSnapshot{
		Tier: testLongTier, ProviderName: testHetznerProvider, Driver: testHetznerProvider, InstanceType: testHetznerType,
		Labels: []string{testAutoLabel}, Healthy: true, MaxInstances: 2,
		ActiveWorkers: 2, IdleWorkers: []orchestrator.WorkerView{
			{InstanceID: "near-rollover", CreatedAt: now.Add(-55 * time.Minute)},
			{InstanceID: "paid-window", CreatedAt: now.Add(-5 * time.Minute)},
		},
	}
	score := scoreCandidate(snapshot, storage.PriceQuote{
		ID: 1, Currency: testEUR, PerHourNanos: 1_000_000_000,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
	}, testUnitFX, []string{testAutoLabel}, 10*time.Minute, time.Minute, time.Minute, now)
	if score.IdleInstanceID != "paid-window" || score.NativeCostNanos != 0 {
		t.Fatalf("score = %+v", score)
	}
	snapshots := map[string]orchestrator.RoutingTierSnapshot{testLongTier: snapshot}
	scheduling := &schedulingState{lanes: map[string][]optimizationLane{}, queuedByRoute: map[string]int{}}
	consumeCapacity(snapshots, scheduling, testRouteName, score)
	remaining := snapshots[testLongTier].IdleWorkers
	if len(remaining) != 1 || remaining[0].InstanceID != "near-rollover" {
		t.Fatalf("remaining idle workers = %+v", remaining)
	}
}
