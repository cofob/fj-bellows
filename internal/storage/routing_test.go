package storage

import (
	"fmt"
	"testing"
	"time"
)

const (
	testRouteName        = "amd64"
	testRoutingJobName   = "build"
	testRoutingLabel     = "ci-auto-amd64"
	testRoutingShortTier = "short"
	testRoutingSource    = "forgejo:test"
	testRoutingWorkflow  = "ci.yml"
)

func TestWorkflowProfileUsesNormalWorkflowJobCompletions(t *testing.T) {
	database := openTestSQLite(t)
	base := time.Date(2026, time.July, 22, 8, 0, 0, 0, time.UTC)
	for i := 1; i <= 10; i++ {
		runtime := time.Duration(i) * time.Minute
		if i == 10 {
			runtime = 20 * time.Minute
		}
		_, err := database.UpsertJob(t.Context(), Job{
			Source: testRoutingSource, Handle: fmt.Sprintf("profile-%d", i),
			RepositoryID: 7, WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName, Tier: testRoutingShortTier,
			Status: JobSucceeded, FirstSeenAt: base, RunnerStartedAt: base,
			RunnerFinishedAt: base.Add(runtime), CompletedAt: base.Add(runtime), UpdatedAt: base.Add(runtime),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := database.UpsertJob(t.Context(), Job{
		Source: testRoutingSource, Handle: "cancelled", RepositoryID: 7,
		WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName, Tier: testRoutingShortTier, Status: JobCancelled,
		FirstSeenAt: base, RunnerStartedAt: base, RunnerFinishedAt: base.Add(3 * time.Hour),
		CompletedAt: base.Add(3 * time.Hour), UpdatedAt: base.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range []Job{
		{Source: testRoutingSource, Handle: "normal-failure", RepositoryID: 7, WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName, Tier: testRoutingShortTier, Status: JobFailed, FirstSeenAt: base, RunnerStartedAt: base, RunnerFinishedAt: base.Add(11 * time.Minute), CompletedAt: base.Add(11 * time.Minute), UpdatedAt: base.Add(11 * time.Minute)},
		{Source: testRoutingSource, Handle: "infra-failure", RepositoryID: 7, WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName, Tier: testRoutingShortTier, Status: JobInfraFailed, FirstSeenAt: base, RunnerStartedAt: base, RunnerFinishedAt: base.Add(4 * time.Hour), CompletedAt: base.Add(4 * time.Hour), UpdatedAt: base.Add(4 * time.Hour)},
		{Source: testRoutingSource, Handle: "other-job", RepositoryID: 7, WorkflowFile: testRoutingWorkflow, JobName: "deploy", Tier: testRoutingShortTier, Status: JobSucceeded, FirstSeenAt: base, RunnerStartedAt: base, RunnerFinishedAt: base.Add(5 * time.Hour), CompletedAt: base.Add(5 * time.Hour), UpdatedAt: base.Add(5 * time.Hour)},
		{Source: testRoutingSource, Handle: "other-tier", RepositoryID: 7, WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName, Tier: testTierName, Status: JobSucceeded, FirstSeenAt: base, RunnerStartedAt: base, RunnerFinishedAt: base.Add(30 * time.Minute), CompletedAt: base.Add(30 * time.Minute), UpdatedAt: base.Add(30 * time.Minute)},
	} {
		if _, extraErr := database.UpsertJob(t.Context(), extra); extraErr != nil {
			t.Fatal(extraErr)
		}
	}
	profile, err := database.WorkflowProfile(t.Context(), WorkflowProfileFilter{
		Source: testRoutingSource, RepositoryID: 7, Workflow: testRoutingWorkflow, JobName: testRoutingJobName,
		Tier: testRoutingShortTier, From: base.Add(-time.Hour), To: base.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Samples != 11 || profile.P95 != 20*time.Minute {
		t.Fatalf("tier WorkflowProfile = %+v, want 11 samples and 20m P95", profile)
	}
	global, err := database.WorkflowProfile(t.Context(), WorkflowProfileFilter{
		Source: testRoutingSource, RepositoryID: 7, Workflow: testRoutingWorkflow, JobName: testRoutingJobName,
		From: base.Add(-time.Hour), To: base.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if global.Samples != 12 || global.P95 != 30*time.Minute {
		t.Fatalf("global WorkflowProfile = %+v, want 12 samples and 30m P95", global)
	}
}

//nolint:gocyclo,funlen // One durable scenario covers assignment, lease, outcome, reporting, and retention together.
func TestRoutingAssignmentReplayAndEffectiveness(t *testing.T) {
	database := openTestSQLite(t)
	base := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	job, err := database.UpsertJob(t.Context(), Job{
		Source: testRoutingSource, Handle: "auto-1", ForgejoJobID: 101,
		RepositoryID: 7, Repository: testRepository, WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName,
		Status: JobObserved, FirstSeenAt: base, QueuedAt: base, UpdatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	quote, err := database.RecordPriceQuote(t.Context(), PriceQuote{
		Provider: testProviderDriver, Driver: testProviderDriver, InstanceType: testInstanceType, Currency: testCurrency,
		PerHourNanos: 1_000_000_000, Source: "test", ObservedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := database.ObserveRoutingDecision(t.Context(), RoutingDecision{
		JobID: job.ID, Route: testRouteName, RequiredLabel: testRoutingLabel,
		PayloadJSON: `{"handle":"auto-1"}`, FallbackTier: testRoutingShortTier,
		FirstSeenAt: base, PolicyVersion: "v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	decision.ProfileSource = "cold_fallback"
	decision.PredictedP95 = 15 * time.Minute
	decision.SelectedTier, decision.SelectedProvider, decision.SelectedDriver = testTierName, testProviderDriver, testProviderDriver
	decision.SelectionReason, decision.ScoreCurrency = "fallback", "USD"
	decision.SelectedIdle = true
	decision.SelectedCostKnown, decision.SelectedCostNanos = true, 100
	decision.FallbackCostKnown, decision.FallbackCostNanos = true, 160
	decision.DecidedAt, decision.ExpiresAt = base.Add(time.Minute), base.Add(5*time.Minute)
	decision.OptimizationQueued, decision.OptimizationActive = true, true
	decision.SelectedWorkerID = "paid-worker"
	decision.ScheduledStartAt, decision.ScheduledFinishAt = base.Add(3*time.Minute), base.Add(20*time.Minute)
	decision.OptimizationQueuePosition, decision.OptimizationWait = 1, 2*time.Minute
	if err := database.AssignRoutingDecision(t.Context(), decision, []RoutingCandidateScore{{
		Tier: testTierName, Provider: testProviderDriver, Driver: testProviderDriver, InstanceType: testInstanceType,
		Eligible: true, IdleWorkers: 1, ActiveWorkers: 1, MaxInstances: 10,
		UsedIdle: true, PredictedRun: 15 * time.Minute, PriceQuoteID: quote.ID,
		NativeCurrency: testCurrency, NativeCostKnown: true, NativeCostNanos: 90,
		FXRate: "1.1", ScoreCostKnown: true, ScoreCostNanos: 100, Rank: 1, Selected: true,
		OptimizationQueued: true, ScheduledStartAt: decision.ScheduledStartAt,
		ScheduledFinishAt: decision.ScheduledFinishAt, OptimizationQueuePosition: 1,
		OptimizationWait: 2 * time.Minute,
	}}); err != nil {
		t.Fatal(err)
	}
	pending, err := database.PendingRoutedJobs(t.Context(), testTierName, base.Add(2*time.Minute))
	if err != nil || len(pending) != 1 || pending[0].JobID != job.ID || !pending[0].OptimizationQueued {
		t.Fatalf("PendingRoutedJobs = %+v, %v", pending, err)
	}
	queue, err := database.QueueSnapshot(t.Context(), base.Add(2*time.Minute))
	if err != nil || len(queue) != 1 || !queue[0].OptimizationQueued ||
		queue[0].SelectedWorkerID != "paid-worker" || queue[0].Tier != testTierName {
		t.Fatalf("QueueSnapshot = %+v, %v", queue, err)
	}
	reservations, err := database.RoutingReservations(t.Context(), base.Add(2*time.Minute))
	if err != nil || len(reservations) != 1 || reservations[0].WorkerID != "paid-worker" ||
		reservations[0].OptimizationQueuePosition != 1 {
		t.Fatalf("RoutingReservations = %+v, %v", reservations, err)
	}
	if err := database.ReleaseRoutingOptimization(t.Context(), job.ID, base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	pending, err = database.PendingRoutedJobs(t.Context(), testTierName, base.Add(2*time.Minute))
	if err != nil || len(pending) != 1 || pending[0].OptimizationQueued {
		t.Fatalf("released optimization assignment = %+v, %v", pending, err)
	}
	if err := database.PreserveRoutingAssignments(t.Context(), testRouteName, base.Add(4*time.Minute), base.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if preserved, err := database.PendingRoutedJobs(t.Context(), testTierName, base.Add(6*time.Minute)); err != nil || len(preserved) != 1 {
		t.Fatalf("preserved PendingRoutedJobs = %+v, %v", preserved, err)
	}
	if expired, err := database.PendingRoutedJobs(t.Context(), testTierName, base.Add(11*time.Minute)); err != nil || len(expired) != 0 {
		t.Fatalf("expired PendingRoutedJobs = %+v, %v", expired, err)
	}
	job.Status = JobSucceeded
	job.Tier, job.Provider, job.Driver = testTierName, testProviderDriver, testProviderDriver
	job.RunnerStartedAt, job.RunnerFinishedAt = base.Add(2*time.Minute), base.Add(12*time.Minute)
	job.CompletedAt, job.UpdatedAt = job.RunnerFinishedAt, job.RunnerFinishedAt
	job, err = database.UpsertJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.RecordCost(t.Context(), CostEntry{
		JobID: job.ID, PriceQuoteID: quote.ID, Kind: CostDirectCompute,
		Currency: testCurrency, Nanos: 50, Known: true, Estimated: true,
		StartedAt: job.RunnerStartedAt, EndedAt: job.RunnerFinishedAt, RecordedAt: job.CompletedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.UpsertJob(t.Context(), Job{
		Source: testRoutingSource, Handle: "explicit-job", RepositoryID: 7,
		WorkflowFile: testRoutingWorkflow, JobName: testRoutingJobName, Tier: testTierName, Provider: testProviderDriver,
		Status: JobSucceeded, FirstSeenAt: base, RunnerStartedAt: base.Add(time.Minute),
		RunnerFinishedAt: base.Add(3 * time.Minute), CompletedAt: base.Add(3 * time.Minute),
		UpdatedAt: base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	stats, err := database.Statistics(t.Context(), StatisticsFilter{Route: testRouteName, GroupBy: GroupNone})
	if err != nil || len(stats.Groups) != 1 || stats.Groups[0].Jobs != 1 {
		t.Fatalf("route-filtered Statistics = %+v, %v", stats, err)
	}
	effect, err := database.RoutingEffectiveness(t.Context(), RoutingEffectivenessFilter{Route: testRouteName})
	if err != nil || len(effect) != 1 {
		t.Fatalf("RoutingEffectiveness = %+v, %v", effect, err)
	}
	got := effect[0]
	if got.Decisions != 1 || got.Completed != 1 || got.FallbackDecisions != 1 ||
		got.IdleDecisions != 1 || got.P95Hits != 1 || got.EstimatedSavingsNanos != 60 ||
		got.ActualDirectNanos != 55 || len(got.Selections) != 1 {
		t.Fatalf("RoutingEffectiveness = %+v", got)
	}
	retained, err := database.Retain(t.Context(), base.Add(24*time.Hour))
	if err != nil || retained.Jobs != 2 {
		t.Fatalf("Retain = %+v, %v", retained, err)
	}
	effect, err = database.RoutingEffectiveness(t.Context(), RoutingEffectivenessFilter{Route: testRouteName})
	if err != nil || len(effect) != 0 {
		t.Fatalf("routing metadata did not cascade after retention: %+v, %v", effect, err)
	}
}

func TestRoutingObservationDoesNotRegressClaimedJob(t *testing.T) {
	database := openTestSQLite(t)
	now := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	job, err := database.UpsertJob(t.Context(), Job{
		Source: testRoutingSource, Handle: "claimed", Status: JobAssigned,
		FirstSeenAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err = database.UpsertJob(t.Context(), Job{
		Source: testRoutingSource, Handle: "claimed", Status: JobObserved,
		FirstSeenAt: now, UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != JobAssigned {
		t.Fatalf("late routing observation regressed status to %q", job.Status)
	}
	if pending, err := database.PendingRoutedJobs(t.Context(), "unused", now); err != nil || len(pending) != 0 {
		t.Fatalf("PendingRoutedJobs = %+v, %v", pending, err)
	}
}
