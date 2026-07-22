package storage

import (
	"context"
	"database/sql"
	"time"
)

// DashboardStore is the optional read-only persistence surface used by the
// web dashboard. It is deliberately separate from Store and RoutingStore so
// orchestrator and router mocks do not grow UI-only methods.
type DashboardStore interface {
	QueueSnapshot(ctx context.Context, now time.Time) ([]QueueJob, error)
}

// QueueJob is one durable waiting task, including automatic-routing metadata
// when the job was assigned by the cost-aware router.
type QueueJob struct {
	ID                        int64     `json:"id"`
	Handle                    string    `json:"handle"`
	Repository                string    `json:"repository"`
	Workflow                  string    `json:"workflow"`
	JobName                   string    `json:"job_name"`
	Tier                      string    `json:"tier"`
	Provider                  string    `json:"provider"`
	Route                     string    `json:"route"`
	RoutingState              string    `json:"routing_state"`
	SelectionReason           string    `json:"selection_reason"`
	OptimizationQueued        bool      `json:"optimization_queued"`
	SelectedWorkerID          string    `json:"selected_worker_id"`
	OptimizationQueuePosition int       `json:"optimization_queue_position"`
	OptimizationWait          int64     `json:"optimization_wait_ns"`
	PredictedP95              int64     `json:"predicted_p95_ns"`
	FirstSeenAt               time.Time `json:"first_seen_at"`
	ScheduledStartAt          time.Time `json:"scheduled_start_at,omitzero"`
	ScheduledFinishAt         time.Time `json:"scheduled_finish_at,omitzero"`
}

// QueueSnapshot returns jobs that have been observed but not claimed by a
// worker. Explicit-tier and automatic jobs share one view; routing columns are
// empty for explicit work.
func (s *SQLite) QueueSnapshot(ctx context.Context, now time.Time) ([]QueueJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT j.id, j.handle, j.repository,
COALESCE(NULLIF(j.workflow_file, ''), NULLIF(j.workflow, ''), j.job_name),
j.job_name, COALESCE(NULLIF(d.selected_tier, ''), j.tier),
COALESCE(NULLIF(d.selected_provider, ''), j.provider), COALESCE(d.route, ''),
COALESCE(d.state, ''), COALESCE(d.selection_reason, ''),
COALESCE(d.optimization_queued AND d.optimization_active, 0),
COALESCE(d.selected_worker_id, ''), COALESCE(d.optimization_queue_position, 0),
COALESCE(d.optimization_wait_ns, 0), COALESCE(d.predicted_p95_ns, 0),
j.first_seen_at_ns, d.scheduled_start_at_ns, d.scheduled_finish_at_ns
FROM jobs j LEFT JOIN routing_decisions d ON d.job_id = j.id
WHERE j.completed_at_ns IS NULL AND j.status = ?
AND (d.id IS NULL OR d.state <> ? OR d.expires_at_ns > ?)
ORDER BY j.first_seen_at_ns, j.id`, JobObserved, RoutingAssigned, now.UnixNano())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []QueueJob
	for rows.Next() {
		var item QueueJob
		var firstSeen int64
		var scheduledStart, scheduledFinish sql.NullInt64
		var optimizationQueued int
		if err := rows.Scan(&item.ID, &item.Handle, &item.Repository, &item.Workflow,
			&item.JobName, &item.Tier, &item.Provider, &item.Route, &item.RoutingState,
			&item.SelectionReason, &optimizationQueued, &item.SelectedWorkerID,
			&item.OptimizationQueuePosition, &item.OptimizationWait, &item.PredictedP95,
			&firstSeen, &scheduledStart, &scheduledFinish); err != nil {
			return nil, err
		}
		item.OptimizationQueued = optimizationQueued == 1
		item.FirstSeenAt = time.Unix(0, firstSeen).UTC()
		item.ScheduledStartAt = timeFromNull(scheduledStart)
		item.ScheduledFinishAt = timeFromNull(scheduledFinish)
		result = append(result, item)
	}
	return result, rows.Err()
}

var _ DashboardStore = (*SQLite)(nil)
