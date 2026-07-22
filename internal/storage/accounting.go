package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// BeginSnapshot creates a managed-image build record and its mutation intent
// in one transaction.
func (s *SQLite) BeginSnapshot(ctx context.Context, snapshot Snapshot) (Snapshot, error) {
	for name, value := range map[string]string{
		"operation ID":  snapshot.OperationID,
		"provider":      snapshot.Provider,
		"driver":        snapshot.Driver,
		"tier":          snapshot.Tier,
		"snapshot name": snapshot.Name,
		"fingerprint":   snapshot.Fingerprint,
	} {
		if err := require(name, value); err != nil {
			return Snapshot{}, err
		}
	}
	if snapshot.State == "" {
		snapshot.State = SnapshotBuilding
	}
	snapshot.CreatedAt = normalizeTime(snapshot.CreatedAt)
	snapshot.UpdatedAt = snapshot.CreatedAt

	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO snapshots(
operation_id, provider, driver, tier, external_id, name, fingerprint,
source_resource_id, state, size_bytes, created_at_ns, updated_at_ns,
completed_at_ns, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			snapshot.OperationID, snapshot.Provider, snapshot.Driver, snapshot.Tier,
			nullableString(snapshot.ExternalID), snapshot.Name, snapshot.Fingerprint,
			nullableID(snapshot.SourceResourceID), snapshot.State, snapshot.SizeBytes,
			snapshot.CreatedAt.UnixNano(), snapshot.UpdatedAt.UnixNano(),
			nullableTime(snapshot.CompletedAt), snapshot.Detail)
		if err != nil {
			return err
		}
		snapshot.ID, err = result.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mutations(
operation_id, kind, provider, tier, resource_id, snapshot_id, state, created_at_ns)
VALUES (?, 'create_snapshot', ?, ?, ?, ?, ?, ?)`, snapshot.OperationID,
			snapshot.Provider, snapshot.Tier, nullableID(snapshot.SourceResourceID),
			snapshot.ID, MutationPending, snapshot.CreatedAt.UnixNano())
		return err
	})
	return snapshot, err
}

// ActivateSnapshot atomically makes a completed image active and marks the
// previous active image stale for asynchronous deletion.
func (s *SQLite) ActivateSnapshot(
	ctx context.Context,
	id int64,
	externalID string,
	sizeBytes int64,
	at time.Time,
) error {
	if err := require("external snapshot ID", externalID); err != nil {
		return err
	}
	if sizeBytes < 0 {
		return errors.New("storage: snapshot size cannot be negative")
	}
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE snapshots SET state = ?, updated_at_ns = ?
WHERE state = ? AND id <> ?
AND provider = (SELECT provider FROM snapshots WHERE id = ?)
AND tier = (SELECT tier FROM snapshots WHERE id = ?)`, SnapshotStale,
			at.UnixNano(), SnapshotActive, id, id, id); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE snapshots SET external_id = ?,
size_bytes = ?, state = ?, updated_at_ns = ?, completed_at_ns = ? WHERE id = ?`,
			externalID, sizeBytes, SnapshotActive, at.UnixNano(), at.UnixNano(), id)
		if err != nil {
			return err
		}
		if err := requireAffected(result); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE mutations SET state = ?, external_id = ?,
completed_at_ns = ? WHERE operation_id = (SELECT operation_id FROM snapshots WHERE id = ?)`,
			MutationSucceeded, externalID, at.UnixNano(), id)
		return err
	})
}

// SetSnapshotState records snapshot cleanup or failure state.
func (s *SQLite) SetSnapshotState(
	ctx context.Context,
	id int64,
	state SnapshotState,
	detail string,
	at time.Time,
) error {
	if state == "" {
		return errors.New("storage: snapshot state is required")
	}
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		completedAt := any(nil)
		if state == SnapshotDeleted || state == SnapshotFailed {
			completedAt = at.UnixNano()
		}
		result, err := tx.ExecContext(ctx, `UPDATE snapshots SET state = ?, detail = ?,
updated_at_ns = ?, completed_at_ns = COALESCE(?, completed_at_ns) WHERE id = ?`,
			state, detail, at.UnixNano(), completedAt, id)
		if err != nil {
			return err
		}
		if err := requireAffected(result); err != nil {
			return err
		}
		if state == SnapshotFailed {
			_, err = tx.ExecContext(ctx, `UPDATE mutations SET state = ?, detail = ?, completed_at_ns = ?
WHERE operation_id = (SELECT operation_id FROM snapshots WHERE id = ?)`,
				MutationFailed, detail, at.UnixNano(), id)
		}
		return err
	})
}

const snapshotColumns = `id, operation_id, provider, driver, tier, external_id,
name, fingerprint, source_resource_id, state, size_bytes, created_at_ns,
updated_at_ns, completed_at_ns, detail`

// Snapshots lists managed images matching filter.
func (s *SQLite) Snapshots(ctx context.Context, filter SnapshotFilter) ([]Snapshot, error) {
	var where []string
	var args []any
	if filter.Provider != "" {
		where = append(where, "provider = ?")
		args = append(args, filter.Provider)
	}
	if filter.Tier != "" {
		where = append(where, "tier = ?")
		args = append(args, filter.Tier)
	}
	if filter.Fingerprint != "" {
		where = append(where, "fingerprint = ?")
		args = append(args, filter.Fingerprint)
	}
	if len(filter.States) != 0 {
		placeholders := make([]string, len(filter.States))
		for i, state := range filter.States {
			placeholders[i] = "?"
			args = append(args, state)
		}
		where = append(where, "state IN ("+strings.Join(placeholders, ",")+")")
	}
	query := "SELECT " + snapshotColumns + " FROM snapshots"
	if len(where) != 0 {
		// where contains only package-owned clauses; values remain bound parameters.
		//nolint:gosec // No caller-controlled text is concatenated into SQL.
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY created_at_ns, id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var snapshots []Snapshot
	for rows.Next() {
		snapshot, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, rows.Err()
}

func scanSnapshot(scanner interface{ Scan(...any) error }) (Snapshot, error) {
	var snapshot Snapshot
	var externalID sql.NullString
	var sourceResourceID, completedAt sql.NullInt64
	var createdAt, updatedAt int64
	err := scanner.Scan(&snapshot.ID, &snapshot.OperationID, &snapshot.Provider,
		&snapshot.Driver, &snapshot.Tier, &externalID, &snapshot.Name,
		&snapshot.Fingerprint, &sourceResourceID, &snapshot.State, &snapshot.SizeBytes,
		&createdAt, &updatedAt, &completedAt, &snapshot.Detail)
	snapshot.ExternalID = externalID.String
	snapshot.SourceResourceID = sourceResourceID.Int64
	snapshot.CreatedAt = time.Unix(0, createdAt).UTC()
	snapshot.UpdatedAt = time.Unix(0, updatedAt).UTC()
	snapshot.CompletedAt = timeFromNull(completedAt)
	return snapshot, err
}

// RecordPriceQuote stores an immutable fixed-point catalog observation.
func (s *SQLite) RecordPriceQuote(ctx context.Context, quote PriceQuote) (PriceQuote, error) {
	for name, value := range map[string]string{
		"provider":      quote.Provider,
		"driver":        quote.Driver,
		"instance type": quote.InstanceType,
		"price source":  quote.Source,
	} {
		if err := require(name, value); err != nil {
			return PriceQuote{}, err
		}
	}
	quote.Currency = strings.ToUpper(strings.TrimSpace(quote.Currency))
	if len(quote.Currency) != 3 {
		return PriceQuote{}, fmt.Errorf("storage: currency must be a three-letter ISO code, got %q", quote.Currency)
	}
	if quote.PerHourNanos < 0 || quote.PerMonthNanos < 0 ||
		quote.SnapshotGBMonthNanos < 0 || quote.MinimumChargeNanos < 0 ||
		quote.BillingQuantum < 0 || quote.MinimumDuration < 0 {
		return PriceQuote{}, errors.New("storage: price quote values cannot be negative")
	}
	quote.ObservedAt = normalizeTime(quote.ObservedAt)
	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO price_quotes(
provider, driver, instance_type, currency, per_hour_nanos, per_month_nanos,
snapshot_gb_month_nanos, minimum_charge_nanos, billing_quantum_ns,
minimum_duration_ns, source, observed_at_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, quote.Provider,
			quote.Driver, quote.InstanceType, quote.Currency, quote.PerHourNanos,
			quote.PerMonthNanos, quote.SnapshotGBMonthNanos, quote.MinimumChargeNanos,
			int64(quote.BillingQuantum), int64(quote.MinimumDuration), quote.Source,
			quote.ObservedAt.UnixNano())
		if err != nil {
			return err
		}
		quote.ID, err = result.LastInsertId()
		return err
	})
	return quote, err
}

const priceQuoteColumns = `id, provider, driver, instance_type, currency,
per_hour_nanos, per_month_nanos, snapshot_gb_month_nanos, minimum_charge_nanos,
billing_quantum_ns, minimum_duration_ns, source, observed_at_ns`

// GetPriceQuote retrieves an immutable observation by ID.
func (s *SQLite) GetPriceQuote(ctx context.Context, id int64) (PriceQuote, error) {
	quote, err := scanPriceQuote(s.db.QueryRowContext(ctx,
		"SELECT "+priceQuoteColumns+" FROM price_quotes WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return PriceQuote{}, ErrNotFound
	}
	return quote, err
}

// LatestPriceQuote retrieves the newest observation for an instance type.
func (s *SQLite) LatestPriceQuote(
	ctx context.Context,
	provider, instanceType string,
) (PriceQuote, error) {
	quote, err := scanPriceQuote(s.db.QueryRowContext(ctx, `SELECT `+priceQuoteColumns+`
FROM price_quotes WHERE provider = ? AND instance_type = ?
ORDER BY observed_at_ns DESC, id DESC LIMIT 1`, provider, instanceType))
	if errors.Is(err, sql.ErrNoRows) {
		return PriceQuote{}, ErrNotFound
	}
	return quote, err
}

func scanPriceQuote(scanner interface{ Scan(...any) error }) (PriceQuote, error) {
	var quote PriceQuote
	var billingQuantum, minimumDuration, observedAt int64
	err := scanner.Scan(&quote.ID, &quote.Provider, &quote.Driver,
		&quote.InstanceType, &quote.Currency, &quote.PerHourNanos,
		&quote.PerMonthNanos, &quote.SnapshotGBMonthNanos,
		&quote.MinimumChargeNanos, &billingQuantum, &minimumDuration,
		&quote.Source, &observedAt)
	quote.BillingQuantum = time.Duration(billingQuantum)
	quote.MinimumDuration = time.Duration(minimumDuration)
	quote.ObservedAt = time.Unix(0, observedAt).UTC()
	return quote, err
}

// RecordCost stores a known estimate or an explicit unknown-price coverage
// entry. Unknown entries carry NULL, not zero, in SQLite.
func (s *SQLite) RecordCost(ctx context.Context, entry CostEntry) (CostEntry, error) {
	if entry.Kind == "" {
		return CostEntry{}, errors.New("storage: cost kind is required")
	}
	if entry.Nanos < 0 {
		return CostEntry{}, errors.New("storage: cost cannot be negative")
	}
	entry.Currency = strings.ToUpper(strings.TrimSpace(entry.Currency))
	if entry.Known && len(entry.Currency) != 3 {
		return CostEntry{}, errors.New("storage: known cost requires a three-letter currency")
	}
	if !entry.Known {
		entry.Currency = ""
		entry.Nanos = 0
	}
	entry.StartedAt = normalizeTime(entry.StartedAt)
	entry.EndedAt = normalizeTime(entry.EndedAt)
	entry.RecordedAt = normalizeTime(entry.RecordedAt)
	if entry.EndedAt.Before(entry.StartedAt) {
		return CostEntry{}, errors.New("storage: cost interval ends before it starts")
	}

	err := s.write(ctx, func(tx *sql.Tx) error {
		var nanos any
		if entry.Known {
			nanos = entry.Nanos
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO cost_entries(
resource_id, snapshot_id, job_id, price_quote_id, kind, currency, nanos, known,
estimated, started_at_ns, ended_at_ns, recorded_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING`, nullableID(entry.ResourceID),
			nullableID(entry.SnapshotID), nullableID(entry.JobID), nullableID(entry.PriceQuoteID),
			entry.Kind, entry.Currency, nanos, boolInt(entry.Known), boolInt(entry.Estimated),
			entry.StartedAt.UnixNano(), entry.EndedAt.UnixNano(), entry.RecordedAt.UnixNano())
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 1 {
			entry.ID, err = result.LastInsertId()
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT id FROM cost_entries
WHERE COALESCE(resource_id, 0) = ?
  AND COALESCE(snapshot_id, 0) = ?
  AND COALESCE(job_id, 0) = ?
	AND kind = ? AND started_at_ns = ?
	AND (? = 1 OR ended_at_ns = ?)
	ORDER BY id LIMIT 1`,
			entry.ResourceID, entry.SnapshotID, entry.JobID, entry.Kind,
			entry.StartedAt.UnixNano(), boolInt(isTerminalCostKind(entry.Kind)),
			entry.EndedAt.UnixNano()).Scan(&entry.ID)
	})
	return entry, err
}

// isTerminalCostKind identifies one-shot totals whose interval end is sampled
// while closing an already-completed provider or job operation. A crash after
// the first ledger write can retry that close with a later wall-clock end; the
// first immutable estimate remains authoritative instead of being counted
// twice. Recurring boot, idle, and reset intervals continue to include their
// end in the identity key.
func isTerminalCostKind(kind CostKind) bool {
	switch kind {
	case CostDirectCompute, CostBilledCompute, CostBillingOverhead,
		CostImageBuilder, CostSnapshotStorage:
		return true
	case CostBoot, CostWarmIdle, CostReset:
		return false
	default:
		return false
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
