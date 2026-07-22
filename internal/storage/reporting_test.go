package storage

import (
	"testing"
	"time"
)

//nolint:gocyclo // End-to-end assertions intentionally cover merging, pagination, and filtering together.
func TestJobHistoryMergesIdentityAndPaginatesStably(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	jobs := []Job{
		completedJob("job-1", testTierName, testProviderName, "build.yml", JobSucceeded, base.Add(time.Minute), 10*time.Second, 2*time.Second, 10*time.Second),
		completedJob("job-2", testTierName, testProviderName, "build.yml", JobFailed, base.Add(2*time.Minute), 20*time.Second, 4*time.Second, 30*time.Second),
		completedJob("job-3", "short", "digitalocean-main", "lint.yml", JobCancelled, base.Add(3*time.Minute), 5*time.Second, time.Second, 3*time.Second),
	}
	for index := range jobs {
		stored, err := database.UpsertJob(ctx, jobs[index])
		if err != nil {
			t.Fatal(err)
		}
		jobs[index] = stored
	}

	merged, err := database.UpsertJob(ctx, Job{
		Source: testForgejoSource, Handle: "job-1", RepositoryOwnerID: 8,
		Workflow: "Build display name", IdentityQuality: IdentityExact,
	})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Status != JobSucceeded || merged.Repository != testRepository ||
		merged.RepositoryOwnerID != 8 || merged.IdentityQuality != IdentityExact {
		t.Fatalf("merged job = %+v", merged)
	}

	first, err := database.JobHistory(ctx, HistoryFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Jobs) != 2 || first.Jobs[0].Handle != "job-3" ||
		first.Jobs[1].Handle != "job-2" || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}
	second, err := database.JobHistory(ctx, HistoryFilter{Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Jobs) != 1 || second.Jobs[0].Handle != "job-1" || second.NextCursor != "" {
		t.Fatalf("second page = %+v", second)
	}
	filtered, err := database.JobHistory(ctx, HistoryFilter{
		Tier: testTierName, Workflow: "build.yml", Status: JobFailed,
	})
	if err != nil || len(filtered.Jobs) != 1 || filtered.Jobs[0].Handle != "job-2" {
		t.Fatalf("filtered page = %+v, %v", filtered, err)
	}
	if _, err := database.JobHistory(ctx, HistoryFilter{Cursor: "not-a-cursor"}); err == nil {
		t.Fatal("invalid cursor unexpectedly succeeded")
	}
}

//nolint:gocyclo // End-to-end aggregation assertions cover all related counters in one fixture.
func TestStatisticsGroupTimingsOutcomesCostsAndCoverage(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	jobs := []Job{
		completedJob("job-1", testTierName, testProviderName, "build.yml", JobSucceeded, base.Add(time.Minute), 10*time.Second, 2*time.Second, 10*time.Second),
		completedJob("job-2", testTierName, testProviderName, "build.yml", JobFailed, base.Add(2*time.Minute), 20*time.Second, 4*time.Second, 30*time.Second),
		completedJob("job-3", "short", "digitalocean-main", "lint.yml", JobInfraFailed, base.Add(3*time.Minute), 5*time.Second, time.Second, 3*time.Second),
	}
	for index := range jobs {
		stored, err := database.UpsertJob(ctx, jobs[index])
		if err != nil {
			t.Fatal(err)
		}
		jobs[index] = stored
	}
	if _, err := database.UpsertJob(ctx, Job{
		Source: testForgejoSource, Handle: "observed-only", RepositoryID: 42,
		Repository: testRepository, WorkflowFile: "unclaimed.yml", Status: JobObserved,
		Tier: testTierName, Provider: testProviderName, FirstSeenAt: base.Add(4 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	entries := []CostEntry{
		knownJobCost(jobs[0], 100_000_000),
		knownJobCost(jobs[1], 300_000_000),
		{
			JobID: jobs[2].ID, Kind: CostDirectCompute, Known: false, Estimated: true,
			StartedAt: jobs[2].RunnerStartedAt, EndedAt: jobs[2].RunnerFinishedAt,
			RecordedAt: jobs[2].CompletedAt,
		},
		{
			JobID: jobs[0].ID, Kind: CostWarmIdle, Currency: testCurrency, Nanos: 25_000_000,
			Known: true, Estimated: true, StartedAt: jobs[0].FirstSeenAt,
			EndedAt: jobs[0].CompletedAt, RecordedAt: jobs[0].CompletedAt,
		},
	}
	for _, entry := range entries {
		if _, err := database.RecordCost(ctx, entry); err != nil {
			t.Fatal(err)
		}
	}

	statistics, err := database.Statistics(ctx, StatisticsFilter{GroupBy: GroupWorkflow})
	if err != nil {
		t.Fatal(err)
	}
	if len(statistics.Groups) != 2 {
		t.Fatalf("group count = %d, want 2: %+v", len(statistics.Groups), statistics.Groups)
	}
	build := statistics.Groups[0]
	if build.Key.Workflow != "build.yml" {
		build = statistics.Groups[1]
	}
	if build.Jobs != 2 || build.Completed != 2 || build.Succeeded != 1 || build.Failed != 1 {
		t.Fatalf("build outcomes = %+v", build)
	}
	if build.QueueDuration.Count != 2 || build.QueueDuration.P50 != 10*time.Second ||
		build.QueueDuration.P95 != 20*time.Second || build.RunDuration.P95 != 30*time.Second {
		t.Fatalf("build durations = queue %+v, run %+v", build.QueueDuration, build.RunDuration)
	}
	if build.PricedJobs != 2 || build.UnpricedJobs != 0 || len(build.DirectCosts) != 1 ||
		build.DirectCosts[0].Currency != testCurrency || build.DirectCosts[0].Nanos != 400_000_000 {
		t.Fatalf("build cost coverage = %+v", build)
	}

	var lint StatisticsGroup
	for _, group := range statistics.Groups {
		if group.Key.Workflow == "lint.yml" {
			lint = group
		}
	}
	if lint.InfraFailed != 1 || lint.PricedJobs != 0 || lint.UnpricedJobs != 1 ||
		len(lint.DirectCosts) != 1 || lint.DirectCosts[0].UnknownEntries != 1 {
		t.Fatalf("lint coverage = %+v", lint)
	}
	if len(statistics.FleetCosts) != 3 {
		t.Fatalf("fleet cost groups = %+v", statistics.FleetCosts)
	}

	byTier, err := database.Statistics(ctx, StatisticsFilter{Tier: testTierName, GroupBy: GroupTier})
	if err != nil || len(byTier.Groups) != 1 || byTier.Groups[0].Key.Tier != testTierName {
		t.Fatalf("tier statistics = %+v, %v", byTier, err)
	}
	byProvider, err := database.Statistics(ctx, StatisticsFilter{GroupBy: GroupProvider})
	if err != nil || len(byProvider.Groups) != 2 {
		t.Fatalf("provider statistics = %+v, %v", byProvider, err)
	}
	byDay, err := database.Statistics(ctx, StatisticsFilter{GroupBy: GroupDay})
	if err != nil || len(byDay.Groups) != 1 || byDay.Groups[0].Key.Day != "2026-07-22" {
		t.Fatalf("day statistics = %+v, %v", byDay, err)
	}
	if _, err := database.Statistics(ctx, StatisticsFilter{GroupBy: "invalid"}); err == nil {
		t.Fatal("invalid grouping unexpectedly succeeded")
	}
}

func TestStatisticsIncludesFleetPhaseTimings(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "timing-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, resource.ID, "timing-server", "worker", base, base); err != nil {
		t.Fatal(err)
	}
	phase, err := database.StartPhase(ctx, Phase{
		ResourceID: resource.ID, Kind: PhaseProvisioning, StartedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FinishPhase(ctx, phase.ID, "ready", "", base.Add(15*time.Second)); err != nil {
		t.Fatal(err)
	}
	statistics, err := database.Statistics(ctx, StatisticsFilter{Tier: testTierName})
	if err != nil {
		t.Fatal(err)
	}
	if len(statistics.FleetTimings) != 1 {
		t.Fatalf("FleetTimings = %+v", statistics.FleetTimings)
	}
	timing := statistics.FleetTimings[0]
	if timing.Kind != PhaseProvisioning || timing.Duration.Count != 1 ||
		timing.Duration.Total != 15*time.Second {
		t.Fatalf("fleet timing = %+v", timing)
	}
}

func TestStatisticsAccruesActiveSnapshotStorageWithoutWritingCheckpoints(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	now := time.Now().UTC()
	base := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Add(-48 * time.Hour)
	quote, err := database.RecordPriceQuote(ctx, PriceQuote{
		Provider: testProviderName, Driver: testProviderDriver, InstanceType: testInstanceType,
		Currency: testCurrency, SnapshotGBMonthNanos: 30_000_000_000,
		Source: "catalog", ObservedAt: base,
	})
	if err != nil || quote.ID == 0 {
		t.Fatalf("RecordPriceQuote() = %+v, %v", quote, err)
	}
	snapshot, err := database.BeginSnapshot(ctx, Snapshot{
		OperationID: "active-priced-snapshot", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, Name: "golden", Fingerprint: "active", CreatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateSnapshot(ctx, snapshot.ID, "active-image", 1_000_000_000, base); err != nil {
		t.Fatal(err)
	}
	filter := StatisticsFilter{From: base, To: base.Add(24 * time.Hour), Tier: testTierName}
	first, err := database.Statistics(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.Statistics(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.FleetCosts) != 1 || first.FleetCosts[0].Kind != CostSnapshotStorage ||
		first.FleetCosts[0].Nanos != 1_000_000_000 || first.FleetCosts[0].UnknownEntries != 0 {
		t.Fatalf("first FleetCosts = %+v", first.FleetCosts)
	}
	if len(second.FleetCosts) != 1 || second.FleetCosts[0] != first.FleetCosts[0] {
		t.Fatalf("repeated Statistics changed active snapshot cost: first=%+v second=%+v",
			first.FleetCosts, second.FleetCosts)
	}
}

//nolint:gocyclo // The end-to-end scenario verifies projection, stability, and absence of writes.
func TestStatisticsProjectsActiveResourceBillingWithoutWritingCheckpoints(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2020, 1, 15, 10, 0, 0, 0, time.UTC)
	quote, err := database.RecordPriceQuote(ctx, PriceQuote{
		Provider: testProviderName, Driver: testProviderDriver, InstanceType: testInstanceType,
		Currency: testCurrency, PerHourNanos: 1_000_000_000, PerMonthNanos: 20_000_000_000,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
		Source: "hourly-catalog", ObservedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "active-hourly-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag,
		PriceQuoteID: quote.ID, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, resource.ID, "active-hourly-server", "worker", base, base); err != nil {
		t.Fatal(err)
	}

	filter := StatisticsFilter{From: base, To: base.Add(5 * time.Minute), Tier: testTierName}
	first, err := database.Statistics(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.Statistics(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.FleetCosts) != 1 {
		t.Fatalf("FleetCosts = %+v", first.FleetCosts)
	}
	cost := first.FleetCosts[0]
	if cost.Kind != CostBilledCompute || cost.Currency != testCurrency ||
		cost.Nanos != 1_000_000_000 || cost.UnknownEntries != 0 || !cost.Estimated {
		t.Fatalf("active billed compute = %+v", cost)
	}
	if len(second.FleetCosts) != 1 || second.FleetCosts[0] != cost {
		t.Fatalf("repeated projection changed: first=%+v second=%+v", first.FleetCosts, second.FleetCosts)
	}
	var persisted int
	if err := database.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM cost_entries WHERE resource_id = ? AND kind = ?",
		resource.ID, CostBilledCompute).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != 0 {
		t.Fatalf("active projection wrote %d billed-compute checkpoints", persisted)
	}
}

func TestStatisticsAppliesActiveResourceMonthlyCapPerCalendarMonth(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)
	quote, err := database.RecordPriceQuote(ctx, PriceQuote{
		Provider: testProviderName, Driver: testProviderDriver, InstanceType: testInstanceType,
		Currency: testCurrency, PerHourNanos: 1_000_000_000, PerMonthNanos: 2_000_000_000,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
		Source: "capped-catalog", ObservedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "active-two-month-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag,
		PriceQuoteID: quote.ID, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, resource.ID, "active-two-month-server", "worker", base, base); err != nil {
		t.Fatal(err)
	}

	statistics, err := database.Statistics(ctx, StatisticsFilter{From: base, To: end, Tier: testTierName})
	if err != nil {
		t.Fatal(err)
	}
	var billed int64
	for _, cost := range statistics.FleetCosts {
		if cost.Kind == CostBilledCompute {
			billed += cost.Nanos
		}
	}
	if billed != 4_000_000_000 {
		t.Fatalf("two capped billing months = %d, want 4000000000; rows=%+v", billed, statistics.FleetCosts)
	}
}

func TestStatisticsRoundsActiveResourceOnceAcrossMonthBoundary(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2020, 1, 31, 23, 30, 0, 0, time.UTC)
	end := base.Add(time.Hour)
	quote, err := database.RecordPriceQuote(ctx, PriceQuote{
		Provider: testProviderName, Driver: testProviderDriver, InstanceType: testInstanceType,
		Currency: testCurrency, PerHourNanos: 1_000_000_000, PerMonthNanos: 20_000_000_000,
		BillingQuantum: time.Hour, MinimumDuration: time.Hour,
		Source: "month-boundary-catalog", ObservedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "active-month-boundary-resource", Provider: testProviderName,
		Driver: testProviderDriver, Tier: testTierName, InstanceType: testInstanceType,
		Tag: testTag, PriceQuoteID: quote.ID, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(
		ctx,
		resource.ID,
		"active-month-boundary-server",
		"worker",
		base,
		base,
	); err != nil {
		t.Fatal(err)
	}

	statistics, err := database.Statistics(ctx, StatisticsFilter{
		From: base, To: end, Tier: testTierName,
	})
	if err != nil {
		t.Fatal(err)
	}
	byDay := make(map[string]int64)
	for _, cost := range statistics.FleetCosts {
		if cost.Kind == CostBilledCompute {
			byDay[cost.Day] += cost.Nanos
		}
	}
	if byDay["2020-01-31"] != 500_000_000 || byDay["2020-02-01"] != 500_000_000 ||
		len(byDay) != 2 {
		t.Fatalf("month-boundary billed compute = %v, want one hour split equally", byDay)
	}
}

//nolint:gocyclo // The scenario checks two interval kinds across both sides of a UTC-day boundary.
func TestStatisticsClipsOverlappingIntervalsAndSplitsUTCDays(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2020, 7, 21, 23, 30, 0, 0, time.UTC)
	from, to := base.Add(15*time.Minute), base.Add(45*time.Minute)

	closed, err := database.BeginResource(ctx, Resource{
		OperationID: "spanning-closed-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, closed.ID, "spanning-closed-server", "worker", base, base); err != nil {
		t.Fatal(err)
	}
	historical, err := database.StartPhase(ctx, Phase{
		ResourceID: closed.ID, Kind: PhaseProvisioning, StartedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FinishPhase(ctx, historical.ID, "ready", "", base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.RecordCost(ctx, CostEntry{
		ResourceID: closed.ID, Kind: CostWarmIdle, Currency: testCurrency,
		Nanos: 120, Known: true, Estimated: true, StartedAt: base,
		EndedAt: base.Add(time.Hour), RecordedAt: base.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.CloseResource(ctx, closed.ID, ResourceClosed, "test", base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	active, err := database.BeginResource(ctx, Resource{
		OperationID: "spanning-active-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, active.ID, "spanning-active-server", "worker", base, base); err != nil {
		t.Fatal(err)
	}
	if _, err := database.StartPhase(ctx, Phase{
		ResourceID: active.ID, Kind: PhaseReset, StartedAt: base,
	}); err != nil {
		t.Fatal(err)
	}

	statistics, err := database.Statistics(ctx, StatisticsFilter{
		From: from, To: to, Tier: testTierName,
	})
	if err != nil {
		t.Fatal(err)
	}
	warmByDay := map[string]int64{}
	for _, cost := range statistics.FleetCosts {
		if cost.Kind == CostWarmIdle {
			warmByDay[cost.Day] += cost.Nanos
		}
	}
	if warmByDay["2020-07-21"] != 30 || warmByDay["2020-07-22"] != 30 || len(warmByDay) != 2 {
		t.Fatalf("clipped warm cost by day = %v, want 30/30", warmByDay)
	}

	timingByKindDay := map[string]DurationSummary{}
	for _, timing := range statistics.FleetTimings {
		timingByKindDay[string(timing.Kind)+"/"+timing.Day] = timing.Duration
	}
	for _, kind := range []PhaseKind{PhaseProvisioning, PhaseReset} {
		for _, day := range []string{"2020-07-21", "2020-07-22"} {
			summary, ok := timingByKindDay[string(kind)+"/"+day]
			if !ok || summary.Count != 1 || summary.Total != 15*time.Minute {
				t.Fatalf("%s/%s timing = %+v (present=%v), want one 15m segment; all=%+v",
					kind, day, summary, ok, statistics.FleetTimings)
			}
		}
	}
}

func completedJob(
	handle, tier, providerName, workflow string,
	status JobStatus,
	firstSeen time.Time,
	queue, dispatch, run time.Duration,
) Job {
	dispatched := firstSeen.Add(queue)
	started := dispatched.Add(dispatch)
	finished := started.Add(run)
	return Job{
		Source: testForgejoSource, Handle: handle, ForgejoJobID: int64(len(handle)), Attempt: 1,
		RepositoryID: 42, Repository: testRepository, WorkflowFile: workflow,
		JobName: "job", IdentityQuality: IdentityExact, Tier: tier,
		Provider: providerName, Driver: providerName, Status: status,
		FirstSeenAt: firstSeen, QueuedAt: firstSeen, DispatchedAt: dispatched,
		RunnerStartedAt: started, RunnerFinishedAt: finished, CompletedAt: finished,
		QueueMeasurementSource: testForgejoSource, RunMeasurementSource: "runner",
	}
}

func knownJobCost(job Job, nanos int64) CostEntry {
	return CostEntry{
		JobID: job.ID, Kind: CostDirectCompute, Currency: testCurrency, Nanos: nanos,
		Known: true, Estimated: true, StartedAt: job.RunnerStartedAt,
		EndedAt: job.RunnerFinishedAt, RecordedAt: job.CompletedAt,
	}
}

//nolint:funlen,gocyclo // The scenario intentionally constructs complete and active recovery graphs.
func TestRetentionDeletesCompletedHistoryButProtectsRecoveryRows(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	old := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	cutoff := old.Add(24 * time.Hour)

	oldQuote, err := database.RecordPriceQuote(ctx, PriceQuote{
		Provider: testProviderName, Driver: testProviderDriver, InstanceType: testInstanceType,
		Currency: testCurrency, PerHourNanos: 10, Source: "old", ObservedAt: old,
	})
	if err != nil {
		t.Fatal(err)
	}
	closed, err := database.BeginResource(ctx, Resource{
		OperationID: "old-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag,
		PriceQuoteID: oldQuote.ID, OpenedAt: old,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, closed.ID, "old-server", "old", old, old); err != nil {
		t.Fatal(err)
	}
	generation, err := database.BeginGeneration(ctx, Generation{
		ResourceID: closed.ID, OperationID: "old-generation", StartedAt: old,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetGenerationState(ctx, generation.ID, GenerationClosed, old.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	job, err := database.UpsertJob(ctx, completedJob(
		"old-job", testTierName, testProviderName, "build.yml", JobSucceeded,
		old, time.Second, time.Second, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	job.ResourceID = closed.ID
	job.GenerationID = generation.ID
	job, err = database.UpsertJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	phase, err := database.StartPhase(ctx, Phase{
		ResourceID: closed.ID, GenerationID: generation.ID, JobID: job.ID,
		Kind: PhaseJob, StartedAt: old,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FinishPhase(ctx, phase.ID, "success", "", old.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.RecordCost(ctx, CostEntry{
		ResourceID: closed.ID, JobID: job.ID, PriceQuoteID: oldQuote.ID,
		Kind: CostDirectCompute, Currency: testCurrency, Nanos: 10, Known: true,
		Estimated: true, StartedAt: old, EndedAt: old.Add(time.Minute), RecordedAt: old.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := database.BeginSnapshot(ctx, Snapshot{
		OperationID: "old-snapshot", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, Name: "failed", Fingerprint: "old", SourceResourceID: closed.ID,
		CreatedAt: old,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSnapshotState(ctx, snapshot.ID, SnapshotFailed, "failed", old.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := database.CloseResource(ctx, closed.ID, ResourceClosed, "done", old.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	active, err := database.BeginResource(ctx, Resource{
		OperationID: "active-resource", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag, OpenedAt: old,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, active.ID, "active-server", "active", old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := database.StartPhase(ctx, Phase{
		ResourceID: active.ID, Kind: PhaseReadyIdle, StartedAt: old,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.UpsertJob(ctx, Job{
		Source: testForgejoSource, Handle: "active-job", Status: JobRunning,
		Tier: testTierName, Provider: testProviderName, ResourceID: active.ID, FirstSeenAt: old,
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := database.Retain(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if removed.Jobs != 1 || removed.Resources != 1 || removed.Generations != 1 ||
		removed.Phases != 1 || removed.Snapshots != 1 || removed.Costs != 1 ||
		removed.PriceQuotes != 1 {
		t.Fatalf("Retain() = %+v", removed)
	}
	openResources, err := database.OpenResources(ctx)
	if err != nil || len(openResources) != 1 || openResources[0].ID != active.ID {
		t.Fatalf("OpenResources() = %+v, %v", openResources, err)
	}
	openJobs, err := database.OpenJobs(ctx)
	if err != nil || len(openJobs) != 1 || openJobs[0].Handle != "active-job" {
		t.Fatalf("OpenJobs() = %+v, %v", openJobs, err)
	}
	openPhases, err := database.OpenPhases(ctx)
	if err != nil || len(openPhases) != 1 || openPhases[0].ResourceID != active.ID {
		t.Fatalf("OpenPhases() = %+v, %v", openPhases, err)
	}
	if zero, err := database.Retain(ctx, time.Time{}); err != nil || zero != (RetentionResult{}) {
		t.Fatalf("zero Retain() = %+v, %v", zero, err)
	}
}
