package storage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecordCostReusesRepeatedResourceEntry(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 16, 0, 0, 0, time.UTC)
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "cost-idempotency-resource", Provider: testProviderName,
		Driver: testProviderDriver, Tier: testTierName, InstanceType: testInstanceType,
		Tag: testTag, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, resource.ID, "cost-server", "cost-worker", base, base); err != nil {
		t.Fatal(err)
	}
	entry := CostEntry{
		ResourceID: resource.ID, Kind: CostWarmIdle, Currency: testCurrency,
		Nanos: 1200, Known: true, Estimated: true,
		StartedAt: base, EndedAt: base.Add(20 * time.Minute), RecordedAt: base.Add(20 * time.Minute),
	}

	first, err := database.RecordCost(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordedAt = entry.RecordedAt.Add(time.Minute)
	second, err := database.RecordCost(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == 0 || second.ID != first.ID {
		t.Fatalf("repeated RecordCost IDs = %d, %d; want one stable non-zero ID", first.ID, second.ID)
	}

	var count, recordedAt int64
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(recorded_at_ns)
FROM cost_entries WHERE resource_id = ? AND kind = ?`, resource.ID, CostWarmIdle).
		Scan(&count, &recordedAt); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("resource cost row count = %d, want 1", count)
	}
	if recordedAt != first.RecordedAt.UnixNano() {
		t.Fatalf("stored recorded_at = %d, want original %d", recordedAt, first.RecordedAt.UnixNano())
	}
}

func TestRecordCostReusesRepeatedSnapshotEntry(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 17, 0, 0, 0, time.UTC)
	snapshot, err := database.BeginSnapshot(ctx, Snapshot{
		OperationID: "cost-idempotency-snapshot", Provider: testProviderName,
		Driver: testProviderDriver, Tier: testTierName, Name: "golden-idempotent",
		Fingerprint: "idempotent", CreatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateSnapshot(ctx, snapshot.ID, "image-idempotent", 4_000_000_000, base); err != nil {
		t.Fatal(err)
	}
	entry := CostEntry{
		SnapshotID: snapshot.ID, Kind: CostSnapshotStorage, Currency: testCurrency,
		Nanos: 4000, Known: true, Estimated: true,
		StartedAt: base, EndedAt: base.Add(24 * time.Hour), RecordedAt: base.Add(24 * time.Hour),
	}

	first, err := database.RecordCost(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.EndedAt = entry.EndedAt.Add(time.Hour)
	entry.RecordedAt = entry.EndedAt
	entry.Nanos = 4500
	second, err := database.RecordCost(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == 0 || second.ID != first.ID {
		t.Fatalf("repeated RecordCost IDs = %d, %d; want one stable non-zero ID", first.ID, second.ID)
	}

	var count int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cost_entries
WHERE snapshot_id = ? AND kind = ?`, snapshot.ID, CostSnapshotStorage).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("snapshot cost row count = %d, want 1", count)
	}
}

func TestRecordCostReusesTerminalResourceEntryWithLaterEnd(t *testing.T) {
	database := openTestSQLite(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 22, 18, 0, 0, 0, time.UTC)
	resource, err := database.BeginResource(ctx, Resource{
		OperationID: "terminal-cost-idempotency-resource", Provider: testProviderName,
		Driver: testProviderDriver, Tier: testTierName, InstanceType: testInstanceType,
		Tag: testTag, OpenedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateResource(ctx, resource.ID, "terminal-server", "terminal-worker", base, base); err != nil {
		t.Fatal(err)
	}
	entry := CostEntry{
		ResourceID: resource.ID, Kind: CostBilledCompute, Currency: testCurrency,
		Nanos: 1200, Known: true, Estimated: true,
		StartedAt: base, EndedAt: base.Add(20 * time.Minute), RecordedAt: base.Add(20 * time.Minute),
	}
	first, err := database.RecordCost(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.EndedAt = entry.EndedAt.Add(5 * time.Minute)
	entry.RecordedAt = entry.EndedAt
	entry.Nanos = 1500
	second, err := database.RecordCost(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == 0 || second.ID != first.ID {
		t.Fatalf("terminal RecordCost IDs = %d, %d; want one stable non-zero ID", first.ID, second.ID)
	}

	var count, endedAt, nanos int64
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(ended_at_ns), MIN(nanos)
FROM cost_entries WHERE resource_id = ? AND kind = ?`, resource.ID, CostBilledCompute).
		Scan(&count, &endedAt, &nanos); err != nil {
		t.Fatal(err)
	}
	if count != 1 || endedAt != first.EndedAt.UnixNano() || nanos != first.Nanos {
		t.Fatalf("terminal rows = %d, end = %d, nanos = %d; want 1, %d, %d",
			count, endedAt, nanos, first.EndedAt.UnixNano(), first.Nanos)
	}
}

func TestRecordCostKeepsDistinctNonTerminalEnds(t *testing.T) {
	database := openTestSQLite(t)
	base := time.Date(2026, 7, 22, 19, 0, 0, 0, time.UTC)
	entry := CostEntry{
		Kind: CostReset, Known: false, Estimated: true,
		StartedAt: base, EndedAt: base.Add(time.Minute), RecordedAt: base.Add(time.Minute),
	}
	first, err := database.RecordCost(t.Context(), entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.EndedAt = entry.EndedAt.Add(time.Minute)
	entry.RecordedAt = entry.EndedAt
	second, err := database.RecordCost(t.Context(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == 0 || second.ID == 0 || second.ID == first.ID {
		t.Fatalf("non-terminal RecordCost IDs = %d, %d; want distinct non-zero IDs", first.ID, second.ID)
	}
}

func TestMigrationThreeDeduplicatesLaterTerminalReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	database, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 22, 20, 0, 0, 0, time.UTC)
	first, err := database.RecordCost(t.Context(), CostEntry{
		Kind: CostBilledCompute, Known: false, Estimated: true,
		StartedAt: base, EndedAt: base.Add(time.Minute), RecordedAt: base.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(t.Context(), `DROP INDEX cost_entries_idempotency;
CREATE UNIQUE INDEX cost_entries_idempotency
ON cost_entries(
    COALESCE(resource_id, 0), COALESCE(snapshot_id, 0), COALESCE(job_id, 0),
    kind, started_at_ns, ended_at_ns
)`); err != nil {
		t.Fatal(err)
	}
	second, err := database.RecordCost(t.Context(), CostEntry{
		Kind: CostBilledCompute, Known: false, Estimated: true,
		StartedAt: base, EndedAt: base.Add(2 * time.Minute), RecordedAt: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID {
		t.Fatalf("v2 fixture IDs = %d, %d; want distinct rows before migration", first.ID, second.ID)
	}
	if _, err := database.db.ExecContext(t.Context(), `DROP TABLE routing_candidate_scores;
DROP TABLE routing_decisions;
DROP INDEX jobs_routing_profile;
DELETE FROM schema_migrations WHERE version >= 3;
PRAGMA user_version = 2`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	var count, survivingID int64
	if err := database.db.QueryRowContext(t.Context(), `SELECT COUNT(*), MIN(id)
FROM cost_entries WHERE kind = ?`, CostBilledCompute).Scan(&count, &survivingID); err != nil {
		t.Fatal(err)
	}
	if count != 1 || survivingID != first.ID {
		t.Fatalf("post-migration rows = %d, surviving ID = %d; want 1, %d", count, survivingID, first.ID)
	}
}
