package orchestrator

import (
	"path/filepath"
	"testing"
	"time"

	omock "github.com/hstern/fj-bellows/internal/orchestrator/mock"
	"github.com/hstern/fj-bellows/internal/provider"
	pmock "github.com/hstern/fj-bellows/internal/provider/mock"
	"github.com/hstern/fj-bellows/internal/storage"
)

func TestRecoverStoredSnapshotsCompletesInterruptedCreate(t *testing.T) {
	o, database := imageRecoveryOrchestrator(t)
	created := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	snapshot := beginRecoverySnapshot(t, database, "create-recovery", created)
	image := provider.ManagedImage{
		ID: "image-recovered", Name: snapshot.Name, Fingerprint: snapshot.Fingerprint,
		SizeBytes: 8_000_000_000, CreatedAt: created.Add(time.Minute),
	}

	if err := o.recoverStoredSnapshots(t.Context(), []provider.ManagedImage{image}); err != nil {
		t.Fatal(err)
	}
	records, err := database.Snapshots(t.Context(), storage.SnapshotFilter{Provider: o.cfg.ProviderName, Tier: o.cfg.Tier})
	if err != nil || len(records) != 1 || records[0].State != storage.SnapshotActive ||
		records[0].ExternalID != image.ID {
		t.Fatalf("Snapshots() = %+v, %v", records, err)
	}
	pending, err := database.PendingMutations(t.Context())
	if err != nil || len(pending) != 0 {
		t.Fatalf("PendingMutations() = %+v, %v", pending, err)
	}
}

func TestRecoverStoredSnapshotsFailsMissingInterruptedCreate(t *testing.T) {
	o, database := imageRecoveryOrchestrator(t)
	beginRecoverySnapshot(t, database, "create-missing", time.Now().UTC())

	if err := o.recoverStoredSnapshots(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	records, err := database.Snapshots(t.Context(), storage.SnapshotFilter{Provider: o.cfg.ProviderName, Tier: o.cfg.Tier})
	if err != nil || len(records) != 1 || records[0].State != storage.SnapshotFailed {
		t.Fatalf("Snapshots() = %+v, %v", records, err)
	}
	pending, err := database.PendingMutations(t.Context())
	if err != nil || len(pending) != 0 {
		t.Fatalf("PendingMutations() = %+v, %v", pending, err)
	}
}

func TestRecoverStoredSnapshotsCompletesInterruptedDelete(t *testing.T) {
	o, database := imageRecoveryOrchestrator(t)
	created := time.Now().UTC().Add(-time.Hour)
	snapshot := beginRecoverySnapshot(t, database, "create-before-delete", created)
	if err := database.ActivateSnapshot(t.Context(), snapshot.ID, "deleted-image", 4_000_000_000, created.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := database.SetSnapshotState(t.Context(), snapshot.ID, storage.SnapshotDeleting, "", created.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := database.BeginMutation(t.Context(), storage.Mutation{
		OperationID: "delete-interrupted", Kind: mutationDeleteSnapshot, Provider: o.cfg.ProviderName,
		Tier: o.cfg.Tier, SnapshotID: snapshot.ID, State: storage.MutationPending,
		CreatedAt: created.Add(2 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	if err := o.recoverStoredSnapshots(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	records, err := database.Snapshots(t.Context(), storage.SnapshotFilter{Provider: o.cfg.ProviderName, Tier: o.cfg.Tier})
	if err != nil || len(records) != 1 || records[0].State != storage.SnapshotDeleted {
		t.Fatalf("Snapshots() = %+v, %v", records, err)
	}
	pending, err := database.PendingMutations(t.Context())
	if err != nil || len(pending) != 0 {
		t.Fatalf("PendingMutations() = %+v, %v", pending, err)
	}
}

func imageRecoveryOrchestrator(t *testing.T) (*Orchestrator, *storage.SQLite) {
	t.Helper()
	database, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	cfg := baseConfig()
	cfg.Tier = fleetLongTier
	cfg.ProviderName = fleetHetznerName
	cfg.Driver = fleetHetznerDriver
	cfg.InstanceType = "cx33"
	o := New(cfg, &pmock.Provider{}, &omock.JobSource{}, &omock.Dispatcher{}, nil)
	o.SetStore(database)
	return o, database
}

func beginRecoverySnapshot(t *testing.T, database *storage.SQLite, operationID string, at time.Time) storage.Snapshot {
	t.Helper()
	snapshot, err := database.BeginSnapshot(t.Context(), storage.Snapshot{
		OperationID: operationID, Provider: fleetHetznerName, Driver: fleetHetznerDriver, Tier: fleetLongTier,
		Name: "fjb-long-golden-123", Fingerprint: "1234567890abcdef", State: storage.SnapshotBuilding,
		CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
