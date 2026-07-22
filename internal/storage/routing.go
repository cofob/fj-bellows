package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"
)

// RoutingStore is the durable boundary used by the fleet-level auto-router.
// It remains separate from Store so ordinary orchestrator fakes do not need
// routing methods.
type RoutingStore interface {
	UpsertJob(ctx context.Context, job Job) (Job, error)
	RecordPriceQuote(ctx context.Context, quote PriceQuote) (PriceQuote, error)
	LatestPriceQuote(ctx context.Context, provider, instanceType string) (PriceQuote, error)
	ObserveRoutingDecision(ctx context.Context, decision RoutingDecision) (RoutingDecision, error)
	DeferRoutingDecision(ctx context.Context, jobID int64, reason string, at time.Time) error
	AssignRoutingDecision(ctx context.Context, decision RoutingDecision, candidates []RoutingCandidateScore) error
	RenewRoutingDecision(ctx context.Context, jobID int64, payload string, expiresAt time.Time) error
	PreserveRoutingAssignments(ctx context.Context, route string, now, expiresAt time.Time) error
	PendingRoutedJobs(ctx context.Context, tier string, now time.Time) ([]RoutedJob, error)
	RoutingReservations(ctx context.Context, now time.Time) ([]RoutingReservation, error)
	ReleaseRoutingOptimization(ctx context.Context, jobID int64, at time.Time) error
	WorkflowProfile(ctx context.Context, filter WorkflowProfileFilter) (WorkflowProfile, error)
	PhaseProfile(ctx context.Context, tier string, kind PhaseKind, from, to time.Time) (WorkflowProfile, error)
	RoutingEffectiveness(ctx context.Context, filter RoutingEffectivenessFilter) ([]RoutingEffectiveness, error)
}

// RoutingDecisionState is the durable route assignment lifecycle.
type RoutingDecisionState string

const (
	// RoutingPending means the job has been observed but has no tier yet.
	RoutingPending RoutingDecisionState = "pending"
	// RoutingAssigned means the selected tier and scorecards are immutable.
	RoutingAssigned RoutingDecisionState = "assigned"
)

// RoutingDecision is one immutable job-to-tier choice. Deferral and lease
// fields may advance before dispatch; the selected scorecard never changes.
type RoutingDecision struct {
	ID                        int64
	JobID                     int64
	Route                     string
	RequiredLabel             string
	PayloadJSON               string
	State                     RoutingDecisionState
	ProfileSource             string
	ProfileSamples            int64
	PredictedP95              time.Duration
	HistoryFrom               time.Time
	HistoryTo                 time.Time
	FallbackTier              string
	SelectedTier              string
	SelectedProvider          string
	SelectedDriver            string
	SelectionReason           string
	SelectedIdle              bool
	ScoreCurrency             string
	SelectedCostNanos         int64
	SelectedCostKnown         bool
	FallbackCostNanos         int64
	FallbackCostKnown         bool
	FirstSeenAt               time.Time
	DecidedAt                 time.Time
	ExpiresAt                 time.Time
	DeferCount                int64
	LastDeferral              string
	PolicyVersion             string
	OptimizationQueued        bool
	OptimizationActive        bool
	SelectedWorkerID          string
	ScheduledStartAt          time.Time
	ScheduledFinishAt         time.Time
	OptimizationQueuePosition int
	OptimizationWait          time.Duration
}

// RoutingCandidateScore captures every input used to rank one tier.
type RoutingCandidateScore struct {
	DecisionID                int64
	Tier                      string
	Provider                  string
	Driver                    string
	InstanceType              string
	Eligible                  bool
	Reason                    string
	IdleWorkers               int
	ActiveWorkers             int
	PendingWorkers            int
	MaxInstances              int
	UsedIdle                  bool
	ProfileSource             string
	ProfileSamples            int64
	PredictedRun              time.Duration
	ProvisionP95              time.Duration
	ResetP95                  time.Duration
	IdleInstanceID            string
	IdleCreatedAt             time.Time
	IdlePaidHourEndAt         time.Time
	IdleReapEligibleAt        time.Time
	PriceQuoteID              int64
	NativeCurrency            string
	NativeCostNanos           int64
	NativeCostKnown           bool
	FXRate                    string
	ScoreCostNanos            int64
	ScoreCostKnown            bool
	Rank                      int
	Selected                  bool
	OptimizationQueued        bool
	ScheduledStartAt          time.Time
	ScheduledFinishAt         time.Time
	OptimizationQueuePosition int
	OptimizationWait          time.Duration
}

// RoutedJob is a live durable assignment replayed into a tier's job source.
type RoutedJob struct {
	DecisionID         int64
	JobID              int64
	PayloadJSON        string
	OptimizationQueued bool
}

// RoutingReservation is one non-terminal assignment tied to a reusable
// worker. The router uses these rows to reconstruct paid-window schedules
// after a restart without treating a busy worker as free capacity.
type RoutingReservation struct {
	JobID                     int64
	Route                     string
	Tier                      string
	WorkerID                  string
	ScheduledStartAt          time.Time
	ScheduledFinishAt         time.Time
	OptimizationQueued        bool
	OptimizationActive        bool
	OptimizationQueuePosition int
	Status                    JobStatus
}

// WorkflowProfileFilter identifies one workflow job and optional candidate
// tier over a bounded history window.
type WorkflowProfileFilter struct {
	Source       string
	RepositoryID int64
	Workflow     string
	JobName      string
	Tier         string
	From         time.Time
	To           time.Time
}

// WorkflowProfile is an exact nearest-rank runtime profile.
type WorkflowProfile struct {
	Samples int64
	P95     time.Duration
}

// RoutingEffectivenessFilter selects decisions by decision time.
type RoutingEffectivenessFilter struct {
	From       time.Time
	To         time.Time
	Route      string
	Tier       string
	Provider   string
	Repository string
	Workflow   string
}

// RoutingSelection counts decisions sent to one tier/provider pair.
type RoutingSelection struct {
	Tier     string
	Provider string
	Jobs     int64
}

// RoutingEffectiveness reports reproducible routing outcomes in the route's
// configured score currency.
type RoutingEffectiveness struct {
	Route                  string
	RequiredLabel          string
	Currency               string
	Decisions              int64
	Completed              int64
	FallbackDecisions      int64
	HistoryDecisions       int64
	IdleDecisions          int64
	DeferredDecisions      int64
	P95Hits                int64
	P95Misses              int64
	EstimatedSelectedNanos int64
	EstimatedFallbackNanos int64
	EstimatedSavingsNanos  int64
	ActualDirectNanos      int64
	ActualUnknownEntries   int64
	Selections             []RoutingSelection
}

const routingDecisionColumns = `id, job_id, route, required_label, payload_json,
state, profile_source, profile_samples, predicted_p95_ns, history_from_ns,
history_to_ns, fallback_tier, selected_tier, selected_provider, selected_driver,
selection_reason, selected_idle, score_currency, selected_cost_nanos,
fallback_cost_nanos, first_seen_at_ns, decided_at_ns, expires_at_ns, defer_count,
last_deferral, policy_version, optimization_queued, optimization_active,
selected_worker_id, scheduled_start_at_ns, scheduled_finish_at_ns,
optimization_queue_position, optimization_wait_ns`

// ObserveRoutingDecision idempotently records a queue observation and returns
// the current durable state without replacing an existing assignment.
func (s *SQLite) ObserveRoutingDecision(ctx context.Context, decision RoutingDecision) (RoutingDecision, error) {
	if decision.JobID == 0 || decision.Route == "" || decision.RequiredLabel == "" ||
		decision.PayloadJSON == "" || decision.FallbackTier == "" || decision.PolicyVersion == "" {
		return RoutingDecision{}, errors.New("storage: incomplete routing decision observation")
	}
	decision.FirstSeenAt = normalizeTime(decision.FirstSeenAt)
	if decision.State == "" {
		decision.State = RoutingPending
	}
	err := s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO routing_decisions(
job_id, route, required_label, payload_json, state, fallback_tier,
first_seen_at_ns, policy_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO UPDATE SET payload_json = excluded.payload_json`,
			decision.JobID, decision.Route, decision.RequiredLabel, decision.PayloadJSON,
			decision.State, decision.FallbackTier, decision.FirstSeenAt.UnixNano(), decision.PolicyVersion)
		return err
	})
	if err != nil {
		return RoutingDecision{}, err
	}
	return s.routingDecisionByJob(ctx, decision.JobID)
}

// DeferRoutingDecision records an unavailable-capacity evaluation.
func (s *SQLite) DeferRoutingDecision(ctx context.Context, jobID int64, reason string, at time.Time) error {
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE routing_decisions
SET defer_count = defer_count + 1, last_deferral = ?, expires_at_ns = ?
WHERE job_id = ? AND state = ?`, reason, at.UnixNano(), jobID, RoutingPending)
		if err != nil {
			return err
		}
		return requireAffected(result)
	})
}

// AssignRoutingDecision atomically freezes the selected tier and its complete
// candidate scorecard. Replays of an already-assigned decision are harmless.
func (s *SQLite) AssignRoutingDecision(ctx context.Context, decision RoutingDecision, candidates []RoutingCandidateScore) error {
	if decision.JobID == 0 || decision.SelectedTier == "" || decision.DecidedAt.IsZero() ||
		decision.ExpiresAt.IsZero() || len(candidates) == 0 {
		return errors.New("storage: incomplete routing assignment")
	}
	decision.DecidedAt = normalizeTime(decision.DecidedAt)
	decision.ExpiresAt = normalizeTime(decision.ExpiresAt)
	return s.write(ctx, func(tx *sql.Tx) error {
		var selectedCost, fallbackCost any
		if decision.SelectedCostKnown {
			selectedCost = decision.SelectedCostNanos
		}
		if decision.FallbackCostKnown {
			fallbackCost = decision.FallbackCostNanos
		}
		result, err := tx.ExecContext(ctx, `UPDATE routing_decisions SET
state = ?, profile_source = ?, profile_samples = ?, predicted_p95_ns = ?,
history_from_ns = ?, history_to_ns = ?, selected_tier = ?, selected_provider = ?,
selected_driver = ?, selection_reason = ?, selected_idle = ?, score_currency = ?,
selected_cost_nanos = ?, fallback_cost_nanos = ?, decided_at_ns = ?, expires_at_ns = ?,
optimization_queued = ?, optimization_active = ?, selected_worker_id = ?,
scheduled_start_at_ns = ?, scheduled_finish_at_ns = ?, optimization_queue_position = ?,
optimization_wait_ns = ?
WHERE job_id = ? AND state = ?`, RoutingAssigned, decision.ProfileSource,
			decision.ProfileSamples, int64(decision.PredictedP95), nullableTime(decision.HistoryFrom),
			nullableTime(decision.HistoryTo), decision.SelectedTier, decision.SelectedProvider,
			decision.SelectedDriver, decision.SelectionReason, boolInt(decision.SelectedIdle),
			decision.ScoreCurrency, selectedCost, fallbackCost, decision.DecidedAt.UnixNano(),
			decision.ExpiresAt.UnixNano(), boolInt(decision.OptimizationQueued),
			boolInt(decision.OptimizationActive), decision.SelectedWorkerID,
			nullableTime(decision.ScheduledStartAt), nullableTime(decision.ScheduledFinishAt),
			decision.OptimizationQueuePosition, int64(decision.OptimizationWait),
			decision.JobID, RoutingPending)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			var state RoutingDecisionState
			if err := tx.QueryRowContext(ctx, "SELECT state FROM routing_decisions WHERE job_id = ?", decision.JobID).Scan(&state); err != nil {
				return err
			}
			if state == RoutingAssigned {
				return nil
			}
			return errors.New("storage: routing decision is not pending")
		}
		return insertRoutingCandidates(ctx, tx, decision.JobID, candidates)
	})
}

func insertRoutingCandidates(ctx context.Context, tx *sql.Tx, jobID int64, candidates []RoutingCandidateScore) error {
	var decisionID int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM routing_decisions WHERE job_id = ?", jobID).Scan(&decisionID); err != nil {
		return err
	}
	for _, candidate := range candidates {
		var nativeCost, scoreCost any
		if candidate.NativeCostKnown {
			nativeCost = candidate.NativeCostNanos
		}
		if candidate.ScoreCostKnown {
			scoreCost = candidate.ScoreCostNanos
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO routing_candidate_scores(
decision_id, tier, provider, driver, instance_type, eligible, reason, idle_workers,
active_workers, pending_workers, max_instances, used_idle, profile_source,
profile_samples, predicted_run_ns,
provision_p95_ns, reset_p95_ns, idle_instance_id, idle_created_at_ns,
idle_paid_hour_end_at_ns, idle_reap_eligible_at_ns, price_quote_id,
native_currency, native_cost_nanos, fx_rate, score_cost_nanos, rank, selected,
optimization_queued, scheduled_start_at_ns, scheduled_finish_at_ns,
optimization_queue_position, optimization_wait_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			decisionID, candidate.Tier, candidate.Provider, candidate.Driver,
			candidate.InstanceType, boolInt(candidate.Eligible), candidate.Reason,
			candidate.IdleWorkers, candidate.ActiveWorkers, candidate.PendingWorkers,
			candidate.MaxInstances, boolInt(candidate.UsedIdle), candidate.ProfileSource,
			candidate.ProfileSamples, int64(candidate.PredictedRun),
			int64(candidate.ProvisionP95), int64(candidate.ResetP95), candidate.IdleInstanceID,
			nullableTime(candidate.IdleCreatedAt), nullableTime(candidate.IdlePaidHourEndAt),
			nullableTime(candidate.IdleReapEligibleAt), nullableID(candidate.PriceQuoteID), candidate.NativeCurrency, nativeCost,
			candidate.FXRate, scoreCost, candidate.Rank, boolInt(candidate.Selected),
			boolInt(candidate.OptimizationQueued), nullableTime(candidate.ScheduledStartAt),
			nullableTime(candidate.ScheduledFinishAt), candidate.OptimizationQueuePosition,
			int64(candidate.OptimizationWait))
		if err != nil {
			return err
		}
	}
	return nil
}

// RenewRoutingDecision keeps an observed, not-yet-dispatched assignment live.
func (s *SQLite) RenewRoutingDecision(ctx context.Context, jobID int64, payload string, expiresAt time.Time) error {
	expiresAt = normalizeTime(expiresAt)
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE routing_decisions
SET payload_json = ?, expires_at_ns = ? WHERE job_id = ? AND state = ?`,
			payload, expiresAt.UnixNano(), jobID, RoutingAssigned)
		return err
	})
}

// PreserveRoutingAssignments prevents an upstream polling outage from being
// interpreted as evidence that every assigned queue job vanished. Successful
// polls renew only jobs actually observed, so absent jobs still age out.
func (s *SQLite) PreserveRoutingAssignments(ctx context.Context, route string, now, expiresAt time.Time) error {
	if route == "" {
		return errors.New("storage: routing route is required")
	}
	now, expiresAt = normalizeTime(now), normalizeTime(expiresAt)
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE routing_decisions
SET expires_at_ns = MAX(expires_at_ns, ?) WHERE route = ? AND state = ?
AND expires_at_ns > ?
AND EXISTS (SELECT 1 FROM jobs j WHERE j.id = routing_decisions.job_id
 AND j.completed_at_ns IS NULL AND j.status = ?)`,
			expiresAt.UnixNano(), route, RoutingAssigned, now.UnixNano(), JobObserved)
		return err
	})
}

// PendingRoutedJobs returns unexpired assignments whose job has not begun.
func (s *SQLite) PendingRoutedJobs(ctx context.Context, tier string, now time.Time) ([]RoutedJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.id, d.job_id, d.payload_json,
d.optimization_queued AND d.optimization_active
FROM routing_decisions d JOIN jobs j ON j.id = d.job_id
WHERE d.state = ? AND d.selected_tier = ? AND d.expires_at_ns > ?
AND j.completed_at_ns IS NULL AND j.status = ?
ORDER BY d.first_seen_at_ns, d.id`, RoutingAssigned, tier, now.UnixNano(), JobObserved)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RoutedJob
	for rows.Next() {
		var item RoutedJob
		var optimizationQueued int
		if err := rows.Scan(&item.DecisionID, &item.JobID, &item.PayloadJSON, &optimizationQueued); err != nil {
			return nil, err
		}
		item.OptimizationQueued = optimizationQueued == 1
		result = append(result, item)
	}
	return result, rows.Err()
}

// RoutingReservations returns live assignments which reserve time on an
// already-created worker. It includes the currently running predecessor as
// well as jobs intentionally held behind it.
func (s *SQLite) RoutingReservations(ctx context.Context, now time.Time) ([]RoutingReservation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.job_id, d.route, d.selected_tier,
d.selected_worker_id, d.scheduled_start_at_ns, d.scheduled_finish_at_ns,
d.optimization_queued, d.optimization_active, d.optimization_queue_position, j.status
FROM routing_decisions d JOIN jobs j ON j.id = d.job_id
WHERE d.state = ? AND d.selected_worker_id <> '' AND j.completed_at_ns IS NULL
AND (j.status <> ? OR d.expires_at_ns > ?)
ORDER BY d.selected_worker_id, d.scheduled_start_at_ns, d.id`,
		RoutingAssigned, JobObserved, now.UnixNano())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RoutingReservation
	for rows.Next() {
		var item RoutingReservation
		var start, finish sql.NullInt64
		var queued, active int
		if err := rows.Scan(&item.JobID, &item.Route, &item.Tier, &item.WorkerID,
			&start, &finish, &queued, &active, &item.OptimizationQueuePosition,
			&item.Status); err != nil {
			return nil, err
		}
		item.ScheduledStartAt = timeFromNull(start)
		item.ScheduledFinishAt = timeFromNull(finish)
		item.OptimizationQueued = queued == 1
		item.OptimizationActive = active == 1
		result = append(result, item)
	}
	return result, rows.Err()
}

// ReleaseRoutingOptimization lets an assigned job provision normally when
// its reserved worker vanished or its paid window can no longer fit the job.
// The immutable scorecard and optimization_queued flag remain available for
// effectiveness analysis.
func (s *SQLite) ReleaseRoutingOptimization(ctx context.Context, jobID int64, at time.Time) error {
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE routing_decisions
SET optimization_active = 0, optimization_released_at_ns = ?
WHERE job_id = ? AND optimization_active = 1`, at.UnixNano(), jobID)
		return err
	})
}

func (s *SQLite) routingDecisionByJob(ctx context.Context, jobID int64) (RoutingDecision, error) {
	decision, err := scanRoutingDecision(s.db.QueryRowContext(ctx,
		"SELECT "+routingDecisionColumns+" FROM routing_decisions WHERE job_id = ?", jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return RoutingDecision{}, ErrNotFound
	}
	return decision, err
}

func scanRoutingDecision(scanner interface{ Scan(...any) error }) (RoutingDecision, error) {
	var decision RoutingDecision
	var historyFrom, historyTo, decidedAt, expiresAt, scheduledStart, scheduledFinish sql.NullInt64
	var predicted, firstSeen, optimizationWait int64
	var selectedCost, fallbackCost sql.NullInt64
	var selectedIdle, optimizationQueued, optimizationActive int
	err := scanner.Scan(&decision.ID, &decision.JobID, &decision.Route,
		&decision.RequiredLabel, &decision.PayloadJSON, &decision.State,
		&decision.ProfileSource, &decision.ProfileSamples, &predicted, &historyFrom,
		&historyTo, &decision.FallbackTier, &decision.SelectedTier,
		&decision.SelectedProvider, &decision.SelectedDriver, &decision.SelectionReason,
		&selectedIdle, &decision.ScoreCurrency, &selectedCost, &fallbackCost,
		&firstSeen, &decidedAt, &expiresAt, &decision.DeferCount,
		&decision.LastDeferral, &decision.PolicyVersion, &optimizationQueued,
		&optimizationActive, &decision.SelectedWorkerID, &scheduledStart,
		&scheduledFinish, &decision.OptimizationQueuePosition, &optimizationWait)
	decision.PredictedP95 = time.Duration(predicted)
	decision.HistoryFrom = timeFromNull(historyFrom)
	decision.HistoryTo = timeFromNull(historyTo)
	decision.SelectedIdle = selectedIdle == 1
	decision.SelectedCostNanos, decision.SelectedCostKnown = selectedCost.Int64, selectedCost.Valid
	decision.FallbackCostNanos, decision.FallbackCostKnown = fallbackCost.Int64, fallbackCost.Valid
	decision.FirstSeenAt = time.Unix(0, firstSeen).UTC()
	decision.DecidedAt = timeFromNull(decidedAt)
	decision.ExpiresAt = timeFromNull(expiresAt)
	decision.OptimizationQueued = optimizationQueued == 1
	decision.OptimizationActive = optimizationActive == 1
	decision.ScheduledStartAt = timeFromNull(scheduledStart)
	decision.ScheduledFinishAt = timeFromNull(scheduledFinish)
	decision.OptimizationWait = time.Duration(optimizationWait)
	return decision, err
}

// WorkflowProfile calculates exact nearest-rank P95 for normal completed
// workflow jobs. Infrastructure/cancellation outcomes are deliberately absent.
func (s *SQLite) WorkflowProfile(ctx context.Context, filter WorkflowProfileFilter) (WorkflowProfile, error) {
	if filter.Source == "" || filter.RepositoryID == 0 || filter.Workflow == "" || filter.JobName == "" {
		return WorkflowProfile{}, errors.New("storage: incomplete workflow profile filter")
	}
	query := `SELECT runner_finished_at_ns - runner_started_at_ns FROM jobs
WHERE source = ? AND repository_id = ?
AND COALESCE(NULLIF(workflow_file, ''), NULLIF(workflow, ''), job_name) = ?
AND job_name = ? AND status IN (?, ?)
AND runner_started_at_ns IS NOT NULL AND runner_finished_at_ns IS NOT NULL
AND runner_finished_at_ns >= runner_started_at_ns`
	args := []any{filter.Source, filter.RepositoryID, filter.Workflow, filter.JobName, JobSucceeded, JobFailed}
	if !filter.From.IsZero() {
		query += " AND completed_at_ns >= ?"
		args = append(args, filter.From.UnixNano())
	}
	if !filter.To.IsZero() {
		query += " AND completed_at_ns < ?"
		args = append(args, filter.To.UnixNano())
	}
	if filter.Tier != "" {
		query += " AND tier = ?"
		args = append(args, filter.Tier)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return WorkflowProfile{}, err
	}
	defer func() { _ = rows.Close() }()
	var values []time.Duration
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return WorkflowProfile{}, err
		}
		values = append(values, time.Duration(value))
	}
	if err := rows.Err(); err != nil {
		return WorkflowProfile{}, err
	}
	if len(values) == 0 {
		return WorkflowProfile{}, nil
	}
	slices.Sort(values)
	index := max((95*len(values)+99)/100, 1) - 1
	return WorkflowProfile{Samples: int64(len(values)), P95: values[index]}, nil
}

// PhaseProfile returns exact P95 lifecycle overhead for one tier.
func (s *SQLite) PhaseProfile(ctx context.Context, tier string, kind PhaseKind, from, to time.Time) (WorkflowProfile, error) {
	query := `SELECT p.ended_at_ns - p.started_at_ns FROM phases p
JOIN resources r ON r.id = p.resource_id
WHERE r.tier = ? AND p.kind = ? AND p.ended_at_ns IS NOT NULL
AND p.ended_at_ns >= p.started_at_ns`
	args := []any{tier, kind}
	if !from.IsZero() {
		query += " AND p.ended_at_ns >= ?"
		args = append(args, from.UnixNano())
	}
	if !to.IsZero() {
		query += " AND p.ended_at_ns < ?"
		args = append(args, to.UnixNano())
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return WorkflowProfile{}, err
	}
	defer func() { _ = rows.Close() }()
	var values []time.Duration
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return WorkflowProfile{}, err
		}
		values = append(values, time.Duration(value))
	}
	if err := rows.Err(); err != nil || len(values) == 0 {
		return WorkflowProfile{}, err
	}
	slices.Sort(values)
	index := max((95*len(values)+99)/100, 1) - 1
	return WorkflowProfile{Samples: int64(len(values)), P95: values[index]}, nil
}

// RoutingEffectiveness is implemented in routing_statistics.go.
var _ RoutingStore = (*SQLite)(nil)

func routingSavings(fallback, selected int64) int64 {
	if fallback <= selected {
		return 0
	}
	return fallback - selected
}

func routingSelectionKey(tier, provider string) string {
	return fmt.Sprintf("%s\x00%s", tier, provider)
}
