package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHistoryLimit = 100
	maxHistoryLimit     = 1000
)

const jobColumns = `id, source, handle, forgejo_job_id, attempt, repository_id,
repository_owner_id, repository, workflow, workflow_file, job_name,
identity_quality, tier, provider, driver, resource_id, generation_id, status,
conclusion, infrastructure_failure, queue_measurement_source,
run_measurement_source, first_seen_at_ns, queued_at_ns, dispatched_at_ns,
runner_started_at_ns, runner_finished_at_ns, completed_at_ns, updated_at_ns`

// UpsertJob records observation, assignment, enrichment, timing, or completion.
// Sparse records merge into known values, and a terminal record cannot be
// regressed by a late non-terminal update.
func (s *SQLite) UpsertJob(ctx context.Context, job Job) (Job, error) {
	if err := require("job source", job.Source); err != nil {
		return Job{}, err
	}
	if err := require("job handle", job.Handle); err != nil {
		return Job{}, err
	}
	if job.Status == "" {
		job.Status = JobObserved
	}
	if job.IdentityQuality == "" {
		job.IdentityQuality = IdentityUnknown
	}
	job.FirstSeenAt = normalizeTime(job.FirstSeenAt)
	job.UpdatedAt = normalizeTime(job.UpdatedAt)

	err := s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO jobs(
source, handle, forgejo_job_id, attempt, repository_id, repository_owner_id,
repository, workflow, workflow_file, job_name, identity_quality, tier, provider,
driver, resource_id, generation_id, status, conclusion,
infrastructure_failure, queue_measurement_source, run_measurement_source,
first_seen_at_ns, queued_at_ns, dispatched_at_ns, runner_started_at_ns,
runner_finished_at_ns, completed_at_ns, updated_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source, handle) DO UPDATE SET
forgejo_job_id = COALESCE(excluded.forgejo_job_id, jobs.forgejo_job_id),
attempt = COALESCE(excluded.attempt, jobs.attempt),
repository_id = COALESCE(excluded.repository_id, jobs.repository_id),
repository_owner_id = COALESCE(excluded.repository_owner_id, jobs.repository_owner_id),
repository = COALESCE(NULLIF(excluded.repository, ''), jobs.repository),
workflow = COALESCE(NULLIF(excluded.workflow, ''), jobs.workflow),
workflow_file = COALESCE(NULLIF(excluded.workflow_file, ''), jobs.workflow_file),
job_name = COALESCE(NULLIF(excluded.job_name, ''), jobs.job_name),
identity_quality = CASE WHEN excluded.identity_quality = 'unknown'
    THEN jobs.identity_quality ELSE excluded.identity_quality END,
tier = COALESCE(NULLIF(excluded.tier, ''), jobs.tier),
provider = COALESCE(NULLIF(excluded.provider, ''), jobs.provider),
driver = COALESCE(NULLIF(excluded.driver, ''), jobs.driver),
resource_id = COALESCE(excluded.resource_id, jobs.resource_id),
generation_id = COALESCE(excluded.generation_id, jobs.generation_id),
status = CASE
    WHEN jobs.completed_at_ns IS NOT NULL AND excluded.completed_at_ns IS NULL
        THEN jobs.status
    WHEN jobs.status IN ('assigned', 'running') AND excluded.status = 'observed'
        THEN jobs.status
    ELSE excluded.status END,
conclusion = COALESCE(NULLIF(excluded.conclusion, ''), jobs.conclusion),
infrastructure_failure = COALESCE(NULLIF(excluded.infrastructure_failure, ''), jobs.infrastructure_failure),
queue_measurement_source = COALESCE(NULLIF(excluded.queue_measurement_source, ''), jobs.queue_measurement_source),
run_measurement_source = COALESCE(NULLIF(excluded.run_measurement_source, ''), jobs.run_measurement_source),
first_seen_at_ns = MIN(excluded.first_seen_at_ns, jobs.first_seen_at_ns),
queued_at_ns = COALESCE(excluded.queued_at_ns, jobs.queued_at_ns),
dispatched_at_ns = COALESCE(excluded.dispatched_at_ns, jobs.dispatched_at_ns),
runner_started_at_ns = COALESCE(excluded.runner_started_at_ns, jobs.runner_started_at_ns),
runner_finished_at_ns = COALESCE(excluded.runner_finished_at_ns, jobs.runner_finished_at_ns),
completed_at_ns = COALESCE(excluded.completed_at_ns, jobs.completed_at_ns),
updated_at_ns = MAX(excluded.updated_at_ns, jobs.updated_at_ns)`,
			job.Source, job.Handle, nullableID(job.ForgejoJobID), nullableID(job.Attempt),
			nullableID(job.RepositoryID), nullableID(job.RepositoryOwnerID), job.Repository,
			job.Workflow, job.WorkflowFile, job.JobName, job.IdentityQuality, job.Tier,
			job.Provider, job.Driver, nullableID(job.ResourceID), nullableID(job.GenerationID),
			job.Status, job.Conclusion, job.InfrastructureFailure,
			job.QueueMeasurementSource, job.RunMeasurementSource,
			job.FirstSeenAt.UnixNano(), nullableTime(job.QueuedAt), nullableTime(job.DispatchedAt),
			nullableTime(job.RunnerStartedAt), nullableTime(job.RunnerFinishedAt),
			nullableTime(job.CompletedAt), job.UpdatedAt.UnixNano())
		return err
	})
	if err != nil {
		return Job{}, err
	}
	return s.jobByKey(ctx, JobKey{Source: job.Source, Handle: job.Handle})
}

func (s *SQLite) jobByKey(ctx context.Context, key JobKey) (Job, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM jobs WHERE source = ? AND handle = ?",
		key.Source, key.Handle)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

// OpenJobs returns jobs requiring restart classification or recovery.
func (s *SQLite) OpenJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs
WHERE completed_at_ns IS NULL ORDER BY first_seen_at_ns, id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func scanJob(scanner interface{ Scan(...any) error }) (Job, error) {
	var job Job
	var forgejoID, attempt, repoID, ownerID, resourceID, generationID sql.NullInt64
	var queuedAt, dispatchedAt, runnerStartedAt, runnerFinishedAt, completedAt sql.NullInt64
	var firstSeenAt, updatedAt int64
	err := scanner.Scan(&job.ID, &job.Source, &job.Handle, &forgejoID, &attempt,
		&repoID, &ownerID, &job.Repository, &job.Workflow, &job.WorkflowFile,
		&job.JobName, &job.IdentityQuality, &job.Tier, &job.Provider, &job.Driver,
		&resourceID, &generationID, &job.Status, &job.Conclusion,
		&job.InfrastructureFailure, &job.QueueMeasurementSource,
		&job.RunMeasurementSource, &firstSeenAt, &queuedAt, &dispatchedAt,
		&runnerStartedAt, &runnerFinishedAt, &completedAt, &updatedAt)
	job.ForgejoJobID = forgejoID.Int64
	job.Attempt = attempt.Int64
	job.RepositoryID = repoID.Int64
	job.RepositoryOwnerID = ownerID.Int64
	job.ResourceID = resourceID.Int64
	job.GenerationID = generationID.Int64
	job.FirstSeenAt = time.Unix(0, firstSeenAt).UTC()
	job.QueuedAt = timeFromNull(queuedAt)
	job.DispatchedAt = timeFromNull(dispatchedAt)
	job.RunnerStartedAt = timeFromNull(runnerStartedAt)
	job.RunnerFinishedAt = timeFromNull(runnerFinishedAt)
	job.CompletedAt = timeFromNull(completedAt)
	job.UpdatedAt = time.Unix(0, updatedAt).UTC()
	return job, err
}

// JobHistory returns a stable page ordered by immutable first-seen time.
func (s *SQLite) JobHistory(ctx context.Context, filter HistoryFilter) (JobPage, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	where, args := jobWhere(filter.From, filter.To, filter.Tier, filter.Provider,
		filter.Repository, filter.Workflow, filter.Status)
	if filter.Cursor != "" {
		cursorTime, cursorID, err := decodeHistoryCursor(filter.Cursor)
		if err != nil {
			return JobPage{}, err
		}
		where = append(where, "(first_seen_at_ns < ? OR (first_seen_at_ns = ? AND id < ?))")
		args = append(args, cursorTime.UnixNano(), cursorTime.UnixNano(), cursorID)
	}
	args = append(args, limit+1)

	query := "SELECT " + jobColumns + " FROM jobs"
	if len(where) != 0 {
		// where contains only package-owned clauses; values remain bound parameters.
		//nolint:gosec // No caller-controlled text is concatenated into SQL.
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY first_seen_at_ns DESC, id DESC LIMIT ?"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return JobPage{}, err
	}
	defer func() { _ = rows.Close() }()

	page := JobPage{Jobs: make([]Job, 0, limit)}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return JobPage{}, err
		}
		page.Jobs = append(page.Jobs, job)
	}
	if err := rows.Err(); err != nil {
		return JobPage{}, err
	}
	if len(page.Jobs) > limit {
		page.Jobs = page.Jobs[:limit]
		last := page.Jobs[len(page.Jobs)-1]
		page.NextCursor = encodeHistoryCursor(last.FirstSeenAt, last.ID)
	}
	return page, nil
}

func jobWhere(
	from, to time.Time,
	tier, providerName, repository, workflow string,
	status JobStatus,
) ([]string, []any) {
	var where []string
	var args []any
	if !from.IsZero() {
		where = append(where, "first_seen_at_ns >= ?")
		args = append(args, from.UTC().UnixNano())
	}
	if !to.IsZero() {
		where = append(where, "first_seen_at_ns < ?")
		args = append(args, to.UTC().UnixNano())
	}
	if tier != "" {
		where = append(where, "tier = ?")
		args = append(args, tier)
	}
	if providerName != "" {
		where = append(where, "provider = ?")
		args = append(args, providerName)
	}
	if repository != "" {
		where = append(where, "repository = ?")
		args = append(args, repository)
	}
	if workflow != "" {
		where = append(where, "(workflow = ? OR workflow_file = ? OR (workflow_file = '' AND job_name = ?))")
		args = append(args, workflow, workflow, workflow)
	}
	if status != "" {
		where = append(where, "status = ?")
		args = append(args, status)
	}
	return where, args
}

func encodeHistoryCursor(at time.Time, id int64) string {
	raw := strconv.FormatInt(at.UTC().UnixNano(), 10) + ":" + strconv.FormatInt(id, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeHistoryCursor(cursor string) (time.Time, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("storage: invalid history cursor: %w", err)
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 2 {
		return time.Time{}, 0, errors.New("storage: invalid history cursor")
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, 0, errors.New("storage: invalid history cursor")
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || id <= 0 {
		return time.Time{}, 0, errors.New("storage: invalid history cursor")
	}
	return time.Unix(0, nanos).UTC(), id, nil
}
