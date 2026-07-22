package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/hstern/fj-bellows/internal/storage"
)

//nolint:gocyclo // This integration scenario verifies one complete recovery transaction and its reporting result.
func TestRecoverInterruptedJobsRecordsWorkflowCostReplaySafely(t *testing.T) {
	database, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	base := time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)
	quote, err := database.RecordPriceQuote(t.Context(), storage.PriceQuote{
		Provider: "hetzner-main", Driver: testHetznerDriver, InstanceType: testHetznerType,
		Currency: "EUR", PerHourNanos: 2_000_000_000,
		Source: "test-catalog", ObservedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := database.BeginResource(t.Context(), storage.Resource{
		OperationID: "recovery-resource", Provider: "hetzner-main", Driver: testHetznerDriver,
		Tier: "long", InstanceType: testHetznerType, Tag: "ci-long", PriceQuoteID: quote.ID,
		OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(t.Context(), resource.ID, "server-1", "worker-1", base, base); err != nil {
		t.Fatal(err)
	}
	if _, err := database.UpsertJob(t.Context(), storage.Job{
		Source: "forgejo:test", Handle: "job-1", RepositoryID: 42,
		Repository: "org/repo", WorkflowFile: "build.yml", JobName: "build",
		Tier: "long", Provider: "hetzner-main", Driver: testHetznerDriver,
		ResourceID: resource.ID, Status: storage.JobRunning,
		FirstSeenAt: base, DispatchedAt: base.Add(5 * time.Minute),
		RunnerStartedAt: base.Add(10 * time.Minute), UpdatedAt: base.Add(10 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	recoveredAt := base.Add(40 * time.Minute)
	if err := recoverInterruptedJobs(t.Context(), database, recoveredAt); err != nil {
		t.Fatal(err)
	}
	// A second startup sees no open job and must not create a second terminal
	// cost row.
	if err := recoverInterruptedJobs(t.Context(), database, recoveredAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	page, err := database.JobHistory(t.Context(), storage.HistoryFilter{})
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("JobHistory = %+v, %v", page, err)
	}
	if page.Jobs[0].Status != storage.JobInterrupted ||
		page.Jobs[0].InfrastructureFailure != "daemon_restart" ||
		!page.Jobs[0].CompletedAt.Equal(recoveredAt) {
		t.Fatalf("recovered job = %+v", page.Jobs[0])
	}
	statistics, err := database.Statistics(t.Context(), storage.StatisticsFilter{
		From: base, To: base.Add(time.Hour), GroupBy: storage.GroupWorkflow,
	})
	if err != nil || len(statistics.Groups) != 1 {
		t.Fatalf("Statistics = %+v, %v", statistics, err)
	}
	group := statistics.Groups[0]
	if group.Interrupted != 1 || group.PricedJobs != 1 || len(group.DirectCosts) != 1 {
		t.Fatalf("recovered workflow coverage = %+v", group)
	}
	cost := group.DirectCosts[0]
	if cost.Kind != storage.CostDirectCompute || cost.Currency != "EUR" ||
		cost.Nanos != 1_000_000_000 || cost.Entries != 1 {
		t.Fatalf("recovered workflow cost = %+v", cost)
	}
}
