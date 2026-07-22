package storage

import (
	"context"
	"database/sql"
	"time"
)

// Retain removes only completed history older than completedBefore. Rows that
// participate in startup recovery are protected even when their timestamps are
// old. A zero cutoff is a no-op and represents retain-forever configuration.
func (s *SQLite) Retain(ctx context.Context, completedBefore time.Time) (RetentionResult, error) {
	if completedBefore.IsZero() {
		return RetentionResult{}, nil
	}
	cutoff := completedBefore.UTC().UnixNano()
	var retained RetentionResult
	err := s.write(ctx, func(tx *sql.Tx) error {
		var err error
		retained.Costs, err = deleteCount(ctx, tx, `DELETE FROM cost_entries
WHERE ended_at_ns < ?
AND (resource_id IS NULL OR EXISTS (
    SELECT 1 FROM resources r WHERE r.id = cost_entries.resource_id
    AND r.closed_at_ns IS NOT NULL AND r.state IN ('closed', 'failed', 'vanished')
))
AND (snapshot_id IS NULL OR EXISTS (
    SELECT 1 FROM snapshots s WHERE s.id = cost_entries.snapshot_id
    AND s.state IN ('deleted', 'failed') AND s.completed_at_ns IS NOT NULL
))
AND (job_id IS NULL OR EXISTS (
    SELECT 1 FROM jobs j WHERE j.id = cost_entries.job_id
    AND j.completed_at_ns IS NOT NULL
))`, cutoff)
		if err != nil {
			return err
		}

		retained.Phases, err = deleteCount(ctx, tx, `DELETE FROM phases
WHERE ended_at_ns IS NOT NULL AND ended_at_ns < ?
AND EXISTS (
    SELECT 1 FROM resources r WHERE r.id = phases.resource_id
    AND r.closed_at_ns IS NOT NULL AND r.state IN ('closed', 'failed', 'vanished')
)
AND (job_id IS NULL OR EXISTS (
    SELECT 1 FROM jobs j WHERE j.id = phases.job_id AND j.completed_at_ns IS NOT NULL
))`, cutoff)
		if err != nil {
			return err
		}

		retained.Jobs, err = deleteCount(ctx, tx, `DELETE FROM jobs
WHERE completed_at_ns IS NOT NULL AND completed_at_ns < ?
AND status IN ('succeeded', 'failed', 'cancelled', 'skipped', 'infra_failed', 'interrupted')
AND (resource_id IS NULL OR EXISTS (
    SELECT 1 FROM resources r WHERE r.id = jobs.resource_id
    AND r.closed_at_ns IS NOT NULL AND r.state IN ('closed', 'failed', 'vanished')
))
AND (generation_id IS NULL OR EXISTS (
    SELECT 1 FROM generations g WHERE g.id = jobs.generation_id
    AND g.closed_at_ns IS NOT NULL AND g.state IN ('closed', 'failed')
))`, cutoff)
		if err != nil {
			return err
		}

		retained.Generations, err = deleteCount(ctx, tx, `DELETE FROM generations
WHERE closed_at_ns IS NOT NULL AND closed_at_ns < ?
AND state IN ('closed', 'failed')
AND EXISTS (
    SELECT 1 FROM resources r WHERE r.id = generations.resource_id
    AND r.closed_at_ns IS NOT NULL AND r.state IN ('closed', 'failed', 'vanished')
)
AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.generation_id = generations.id)`, cutoff)
		if err != nil {
			return err
		}

		retained.Snapshots, err = deleteCount(ctx, tx, `DELETE FROM snapshots
WHERE completed_at_ns IS NOT NULL AND completed_at_ns < ?
AND state IN ('deleted', 'failed')
AND NOT EXISTS (
    SELECT 1 FROM mutations m WHERE m.snapshot_id = snapshots.id AND m.state = 'pending'
)`, cutoff)
		if err != nil {
			return err
		}

		retained.Mutations, err = deleteCount(ctx, tx, `DELETE FROM mutations
WHERE completed_at_ns IS NOT NULL AND completed_at_ns < ?
AND state IN ('succeeded', 'failed')
AND (resource_id IS NULL OR NOT EXISTS (
    SELECT 1 FROM resources r WHERE r.id = mutations.resource_id
    AND r.closed_at_ns IS NULL
))
AND (snapshot_id IS NULL OR NOT EXISTS (
    SELECT 1 FROM snapshots s WHERE s.id = mutations.snapshot_id
    AND s.state NOT IN ('deleted', 'failed')
))`, cutoff)
		if err != nil {
			return err
		}

		retained.Resources, err = deleteCount(ctx, tx, `DELETE FROM resources
WHERE closed_at_ns IS NOT NULL AND closed_at_ns < ?
AND state IN ('closed', 'failed', 'vanished')
AND NOT EXISTS (SELECT 1 FROM generations g WHERE g.resource_id = resources.id)
AND NOT EXISTS (SELECT 1 FROM phases p WHERE p.resource_id = resources.id)
AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.resource_id = resources.id)
AND NOT EXISTS (SELECT 1 FROM mutations m WHERE m.resource_id = resources.id AND m.state = 'pending')
AND NOT EXISTS (
    SELECT 1 FROM snapshots s WHERE s.source_resource_id = resources.id
    AND s.state NOT IN ('deleted', 'failed')
)`, cutoff)
		if err != nil {
			return err
		}

		retained.PriceQuotes, err = deleteCount(ctx, tx, `DELETE FROM price_quotes
WHERE observed_at_ns < ?
AND NOT EXISTS (SELECT 1 FROM resources r WHERE r.price_quote_id = price_quotes.id)
AND NOT EXISTS (SELECT 1 FROM cost_entries c WHERE c.price_quote_id = price_quotes.id)`, cutoff)
		return err
	})
	return retained, err
}

func deleteCount(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
