package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testCurrency       = "EUR"
	testForgejoSource  = "forgejo"
	testInstanceType   = "cx33"
	testProviderDriver = "hetzner"
	testProviderName   = "hetzner-main"
	testRepository     = "org/repo"
	testTag            = "fjb-test"
	testTierName       = "long"
)

func openTestSQLite(t *testing.T) *SQLite {
	t.Helper()
	database, err := Open(t.Context(), filepath.Join(t.TempDir(), "storage.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return database
}

func TestOpenConfiguresSQLiteAndMigrates(t *testing.T) {
	database := openTestSQLite(t)
	tests := []struct {
		pragma string
		want   any
	}{
		{pragma: "journal_mode", want: "wal"},
		{pragma: "foreign_keys", want: int64(1)},
		{pragma: "synchronous", want: int64(2)},
		{pragma: "busy_timeout", want: int64(5000)},
		{pragma: "user_version", want: int64(currentSchemaVersion)},
	}
	for _, test := range tests {
		t.Run(test.pragma, func(t *testing.T) {
			switch want := test.want.(type) {
			case string:
				var got string
				if err := database.db.QueryRowContext(t.Context(), "PRAGMA "+test.pragma).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("PRAGMA %s = %v, want %v", test.pragma, got, want)
				}
			case int64:
				var got int64
				if err := database.db.QueryRowContext(t.Context(), "PRAGMA "+test.pragma).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("PRAGMA %s = %v, want %v", test.pragma, got, want)
				}
			}
		})
	}

	var migrations int
	if err := database.db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM schema_migrations").Scan(&migrations); err != nil {
		t.Fatal(err)
	}
	if migrations != currentSchemaVersion {
		t.Fatalf("migration count = %d, want %d", migrations, currentSchemaVersion)
	}
}

func TestOpenCreatesPrivateOnDiskDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private.db")
	database, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("database permissions = %04o, want 0600", got)
	}
}

func TestOpenRejectsInsecureExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "insecure.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil { //nolint:gosec // The test intentionally creates an insecure ledger to verify rejection.
		t.Fatal(err)
	}
	_, err := Open(t.Context(), path)
	if err == nil || !strings.Contains(err.Error(), "require 0600") {
		t.Fatalf("Open() error = %v, want private-permissions error", err)
	}
}

func TestOpenAllowsInMemoryDatabase(t *testing.T) {
	database, err := Open(t.Context(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsNewerSchemaAndReopensCurrentSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	ctx := t.Context()
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen current schema: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", currentSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, path)
	if !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("Open() error = %v, want ErrNewerSchema", err)
	}
}

func TestHealthTracksWriteFailureAndRecovery(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	err := database.BeginMutation(ctx, Mutation{
		OperationID: "invalid-reference", Kind: "destroy", Provider: testProviderName,
		ResourceID: 999,
	})
	if err == nil {
		t.Fatal("foreign-key write unexpectedly succeeded")
	}
	failed := database.Health(ctx)
	if failed.Healthy || failed.LastError == "" {
		t.Fatalf("Health() after failed write = %+v", failed)
	}
	if err := database.BeginMutation(ctx, Mutation{
		OperationID: "valid-intent", Kind: "reconcile", Provider: testProviderName,
	}); err != nil {
		t.Fatal(err)
	}
	recovered := database.Health(ctx)
	if !recovered.Healthy || recovered.LastError != "" || recovered.LastSuccessfulWrite.IsZero() {
		t.Fatalf("Health() after recovery = %+v", recovered)
	}
}

//nolint:gocyclo // This end-to-end recovery fixture validates the related lifecycle records together.
func TestResourceGenerationPhaseAndMutationRecovery(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)

	quote, err := database.RecordPriceQuote(ctx, PriceQuote{
		Provider: testProviderName, Driver: testProviderDriver, InstanceType: testInstanceType,
		Currency: "eur", PerHourNanos: 12_300_000, MinimumChargeNanos: 1_000_000,
		BillingQuantum: time.Hour,
		Source:         "catalog", ObservedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	loadedQuote, err := database.LatestPriceQuote(ctx, testProviderName, testInstanceType)
	if err != nil || loadedQuote.ID != quote.ID || loadedQuote.Currency != testCurrency ||
		loadedQuote.MinimumChargeNanos != 1_000_000 {
		t.Fatalf("LatestPriceQuote() = %+v, %v", loadedQuote, err)
	}

	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "provision-1", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag, PriceQuoteID: quote.ID,
		OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := database.PendingMutations(ctx)
	if err != nil || len(pending) != 1 || pending[0].ResourceID != resource.ID {
		t.Fatalf("PendingMutations() = %+v, %v", pending, err)
	}
	createdAt := base.Add(time.Minute)
	if err := database.ActivateResource(ctx, resource.ID, "server-7", "worker-7", createdAt, createdAt); err != nil {
		t.Fatal(err)
	}
	if pending, err = database.PendingMutations(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("pending after activation = %+v, %v", pending, err)
	}

	generation, err := database.BeginGeneration(ctx, Generation{
		ResourceID: resource.ID, OperationID: "reset-1", ImageID: "snapshot-1",
		Fingerprint: "abc", StartedAt: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if generation.Number != 1 {
		t.Fatalf("generation number = %d, want 1", generation.Number)
	}
	if err := database.SetGenerationState(ctx, generation.ID, GenerationReady, base.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	openGenerations, err := database.OpenGenerations(ctx)
	if err != nil || len(openGenerations) != 1 || openGenerations[0].State != GenerationReady {
		t.Fatalf("OpenGenerations() = %+v, %v", openGenerations, err)
	}

	phase, err := database.StartPhase(ctx, Phase{
		ResourceID: resource.ID, GenerationID: generation.ID,
		Kind: PhaseReadyIdle, StartedAt: base.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	openPhases, err := database.OpenPhases(ctx)
	if err != nil || len(openPhases) != 1 || openPhases[0].ID != phase.ID {
		t.Fatalf("OpenPhases() = %+v, %v", openPhases, err)
	}
	if err := database.FinishPhase(ctx, phase.ID, "assigned", "", base.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := database.SetGenerationState(ctx, generation.ID, GenerationClosed, base.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := database.CloseResource(ctx, resource.ID, ResourceClosed, "reaped", base.Add(6*time.Minute)); err != nil {
		t.Fatal(err)
	}
	openResources, err := database.OpenResources(ctx)
	if err != nil || len(openResources) != 0 {
		t.Fatalf("OpenResources() = %+v, %v", openResources, err)
	}

	health := database.Health(ctx)
	if !health.Healthy || health.LastSuccessfulWrite.IsZero() || health.LastError != "" {
		t.Fatalf("Health() = %+v", health)
	}
}

func TestActivateResourceWithGenerationIsAtomic(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "atomic-provision", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, InstanceType: testInstanceType, Tag: testTag, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := database.ActivateResourceWithGeneration(ctx, resource.ID,
		"atomic-server", "atomic-worker", base, base.Add(time.Second), Generation{
			ResourceID: resource.ID, OperationID: "atomic-generation", ImageID: "image-1",
			Fingerprint: "fingerprint", State: GenerationPreparing, StartedAt: base.Add(time.Second),
		})
	if err != nil {
		t.Fatal(err)
	}
	if generation.ID == 0 || generation.Number != 1 {
		t.Fatalf("generation = %+v, want persisted generation 1", generation)
	}
	resources, err := database.OpenResources(ctx)
	if err != nil || len(resources) != 1 || resources[0].ExternalID != "atomic-server" ||
		resources[0].State != ResourceActive {
		t.Fatalf("OpenResources() = %+v, %v", resources, err)
	}
	generations, err := database.OpenGenerations(ctx)
	if err != nil || len(generations) != 1 || generations[0].ID != generation.ID {
		t.Fatalf("OpenGenerations() = %+v, %v", generations, err)
	}
	pending, err := database.PendingMutations(ctx)
	if err != nil || len(pending) != 1 || pending[0].Kind != "generation" {
		t.Fatalf("PendingMutations() = %+v, %v", pending, err)
	}
}

func TestSnapshotActivationRotatesPreviousImage(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)

	first, err := database.BeginSnapshot(ctx, Snapshot{
		OperationID: "snapshot-op-1", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, Name: "golden-a", Fingerprint: "a", CreatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateSnapshot(ctx, first.ID, "image-1", 10<<30, base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	second, err := database.BeginSnapshot(ctx, Snapshot{
		OperationID: "snapshot-op-2", Provider: testProviderName, Driver: testProviderDriver,
		Tier: testTierName, Name: "golden-b", Fingerprint: "b", CreatedAt: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateSnapshot(ctx, second.ID, "image-2", 11<<30, base.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}

	snapshots, err := database.Snapshots(ctx, SnapshotFilter{Provider: testProviderName, Tier: testTierName})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || snapshots[0].State != SnapshotStale || snapshots[1].State != SnapshotActive {
		t.Fatalf("Snapshots() = %+v", snapshots)
	}
	pending, err := database.PendingMutations(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("PendingMutations() = %+v, %v", pending, err)
	}
}

func TestConcurrentJobUpsertsAreSafe(t *testing.T) {
	database := openTestSQLite(t)
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	const count = 32
	var wait sync.WaitGroup
	for index := range count {
		wait.Go(func() {
			_, err := database.UpsertJob(context.Background(), Job{
				Source: testForgejoSource, Handle: fmt.Sprintf("job-%d", index),
				Status: JobObserved, FirstSeenAt: base.Add(time.Duration(index) * time.Second),
			})
			if err != nil {
				t.Errorf("UpsertJob() error = %v", err)
			}
		})
	}
	wait.Wait()
	jobs, err := database.OpenJobs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != count {
		t.Fatalf("OpenJobs() count = %d, want %d", len(jobs), count)
	}
}
