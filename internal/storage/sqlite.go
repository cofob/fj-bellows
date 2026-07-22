package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	// Register the CGO-free SQLite database/sql driver.
	_ "modernc.org/sqlite"
)

const defaultBusyTimeout = 5 * time.Second

// Options controls SQLite connection behavior.
type Options struct {
	BusyTimeout time.Duration
}

// SQLite is a concurrency-safe Store backed by database/sql and modernc
// SQLite. One pooled connection makes connection-local PRAGMAs deterministic;
// WAL still allows external readers to proceed during writes.
type SQLite struct {
	db *sql.DB

	healthMu  sync.RWMutex
	lastWrite time.Time
	lastError string
}

var (
	_ Store                       = (*SQLite)(nil)
	_ ResourceGenerationActivator = (*SQLite)(nil)
)

// Open opens and migrates path with production defaults.
func Open(ctx context.Context, path string) (*SQLite, error) {
	return OpenWithOptions(ctx, path, Options{})
}

// OpenWithOptions opens and migrates path. It does not create the parent
// directory, so deployment mistakes fail at startup instead of writing to an
// unexpected location.
func OpenWithOptions(ctx context.Context, path string, opts Options) (*SQLite, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("storage: database path is required")
	}
	if err := prepareDatabaseFile(path); err != nil {
		return nil, err
	}
	if opts.BusyTimeout <= 0 {
		opts.BusyTimeout = defaultBusyTimeout
	}

	db, err := sql.Open("sqlite", sqliteDSN(path, opts))
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &SQLite{db: db}
	if err := s.configure(ctx, opts); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.loadLastWrite(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// prepareDatabaseFile ensures an on-disk ledger is never created with
// permissions that expose CI history to other local users. Existing ledgers
// are rejected instead of silently chmodded so an operator must explicitly
// acknowledge and correct an insecure deployment.
func prepareDatabaseFile(path string) error {
	if path == ":memory:" {
		return nil
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // fixed private ledger mode.
	if err == nil {
		// OpenFile applies the process umask. Restore the exact contract even
		// under an unusually restrictive umask before SQLite reopens the path.
		if chmodErr := file.Chmod(0o600); chmodErr != nil {
			_ = file.Close()
			return fmt.Errorf("storage: secure database permissions: %w", chmodErr)
		}
	} else if errors.Is(err, os.ErrExist) {
		file, err = os.OpenFile(path, os.O_RDWR, 0) //nolint:gosec // operator-configured ledger path.
	}
	if err != nil {
		return fmt.Errorf("storage: prepare database file: %w", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("storage: inspect database file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("storage: database path must be a regular file, got %s", info.Mode().Type())
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		return fmt.Errorf("storage: database file permissions are %04o; require 0600", permissions)
	}
	return nil
}

func sqliteDSN(path string, opts Options) string {
	pragmas := url.Values{}
	pragmas.Add("_pragma", "foreign_keys(1)")
	pragmas.Add("_pragma", "journal_mode(WAL)")
	pragmas.Add("_pragma", "synchronous(FULL)")
	pragmas.Add("_pragma", "busy_timeout("+strconv.FormatInt(opts.BusyTimeout.Milliseconds(), 10)+")")
	if path == ":memory:" {
		return "file::memory:?" + pragmas.Encode()
	}
	return (&url.URL{Scheme: "file", Path: path, RawQuery: pragmas.Encode()}).String()
}

func (s *SQLite) configure(ctx context.Context, opts Options) error {
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA busy_timeout = " + strconv.FormatInt(opts.BusyTimeout.Milliseconds(), 10),
	}
	for _, statement := range pragmas {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("storage: configure %q: %w", statement, err)
		}
	}
	return nil
}

func (s *SQLite) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("storage: read schema version: %w", err)
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("%w: database=%d binary=%d", ErrNewerSchema, version, currentSchemaVersion)
	}

	for next := version + 1; next <= currentSchemaVersion; next++ {
		if _, err := tx.ExecContext(ctx, migrations[next-1]); err != nil {
			return fmt.Errorf("storage: apply migration %d: %w", next, err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO schema_migrations(version, applied_at_ns) VALUES (?, ?)",
			next, time.Now().UTC().UnixNano()); err != nil {
			return fmt.Errorf("storage: record migration %d: %w", next, err)
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = "+strconv.Itoa(next)); err != nil {
			return fmt.Errorf("storage: set schema version %d: %w", next, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit migrations: %w", err)
	}
	return nil
}

func (s *SQLite) loadLastWrite(ctx context.Context) error {
	var raw string
	err := s.db.QueryRowContext(ctx,
		"SELECT value FROM storage_meta WHERE key = 'last_successful_write_ns'").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: read last write: %w", err)
	}
	nanos, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("storage: parse last write: %w", err)
	}
	s.lastWrite = time.Unix(0, nanos).UTC()
	return nil
}

// Close closes the underlying database pool.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// Health checks the database and reports write durability state.
func (s *SQLite) Health(ctx context.Context) Health {
	pingErr := s.db.PingContext(ctx)
	s.healthMu.RLock()
	health := Health{
		Healthy:             pingErr == nil && s.lastError == "",
		LastSuccessfulWrite: s.lastWrite,
		LastError:           s.lastError,
	}
	s.healthMu.RUnlock()
	if pingErr != nil {
		health.Healthy = false
		health.LastError = pingErr.Error()
	}
	return health
}

func (s *SQLite) write(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.setWriteError(err)
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := fn(tx); err != nil {
		s.setWriteError(err)
		return err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO storage_meta(key, value)
VALUES ('last_successful_write_ns', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, strconv.FormatInt(now.UnixNano(), 10)); err != nil {
		s.setWriteError(err)
		return err
	}
	if err := tx.Commit(); err != nil {
		s.setWriteError(err)
		return err
	}

	s.healthMu.Lock()
	s.lastWrite = now
	s.lastError = ""
	s.healthMu.Unlock()
	return nil
}

func (s *SQLite) setWriteError(err error) {
	s.healthMu.Lock()
	s.lastError = err.Error()
	s.healthMu.Unlock()
}

func normalizeTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().UnixNano()
}

func timeFromNull(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.Unix(0, value.Int64).UTC()
}

func nullableID(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func require(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("storage: %s is required", name)
	}
	return nil
}

// BeginMutation durably records intent before an external side effect.
func (s *SQLite) BeginMutation(ctx context.Context, mutation Mutation) error {
	if err := require("operation ID", mutation.OperationID); err != nil {
		return err
	}
	if err := require("mutation kind", mutation.Kind); err != nil {
		return err
	}
	if err := require("provider", mutation.Provider); err != nil {
		return err
	}
	if mutation.State == "" {
		mutation.State = MutationPending
	}
	mutation.CreatedAt = normalizeTime(mutation.CreatedAt)
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO mutations(
operation_id, kind, provider, tier, resource_id, snapshot_id, state,
external_id, detail, created_at_ns, completed_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			mutation.OperationID, mutation.Kind, mutation.Provider, mutation.Tier,
			nullableID(mutation.ResourceID), nullableID(mutation.SnapshotID), mutation.State,
			mutation.ExternalID, mutation.Detail, mutation.CreatedAt.UnixNano(),
			nullableTime(mutation.CompletedAt))
		return err
	})
}

// FinishMutation records the externally observed result of an intent.
func (s *SQLite) FinishMutation(
	ctx context.Context,
	operationID string,
	state MutationState,
	externalID, detail string,
	at time.Time,
) error {
	if err := require("operation ID", operationID); err != nil {
		return err
	}
	if state != MutationSucceeded && state != MutationFailed {
		return fmt.Errorf("storage: terminal mutation state required, got %q", state)
	}
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE mutations
SET state = ?, external_id = ?, detail = ?, completed_at_ns = ?
WHERE operation_id = ?`, state, externalID, detail, at.UnixNano(), operationID)
		if err != nil {
			return err
		}
		return requireAffected(result)
	})
}

// PendingMutations returns incomplete side effects for startup reconciliation.
func (s *SQLite) PendingMutations(ctx context.Context) ([]Mutation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id, kind, provider, tier,
resource_id, snapshot_id, state, external_id, detail, created_at_ns, completed_at_ns
FROM mutations WHERE state = ? ORDER BY created_at_ns, operation_id`, MutationPending)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var mutations []Mutation
	for rows.Next() {
		mutation, err := scanMutation(rows)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, rows.Err()
}

func scanMutation(scanner interface{ Scan(...any) error }) (Mutation, error) {
	var mutation Mutation
	var resourceID, snapshotID, completedAt sql.NullInt64
	var createdAt int64
	err := scanner.Scan(&mutation.OperationID, &mutation.Kind, &mutation.Provider,
		&mutation.Tier, &resourceID, &snapshotID, &mutation.State,
		&mutation.ExternalID, &mutation.Detail, &createdAt, &completedAt)
	mutation.ResourceID = resourceID.Int64
	mutation.SnapshotID = snapshotID.Int64
	mutation.CreatedAt = time.Unix(0, createdAt).UTC()
	mutation.CompletedAt = timeFromNull(completedAt)
	return mutation, err
}

// BeginResource creates a provider allocation intent and its pending provision
// mutation in one transaction.
func (s *SQLite) BeginResource(ctx context.Context, resource Resource) (Resource, error) {
	for name, value := range map[string]string{
		"operation ID":  resource.OperationID,
		"provider":      resource.Provider,
		"driver":        resource.Driver,
		"tier":          resource.Tier,
		"instance type": resource.InstanceType,
		"ownership tag": resource.Tag,
	} {
		if err := require(name, value); err != nil {
			return Resource{}, err
		}
	}
	if resource.State == "" {
		resource.State = ResourceProvisioning
	}
	resource.OpenedAt = normalizeTime(resource.OpenedAt)
	resource.UpdatedAt = resource.OpenedAt

	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO resources(
operation_id, provider, driver, tier, external_id, name, instance_type,
ownership_tag, state, price_quote_id, provider_created_at_ns, opened_at_ns,
updated_at_ns, closed_at_ns, close_reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			resource.OperationID, resource.Provider, resource.Driver, resource.Tier,
			nullableString(resource.ExternalID), resource.Name, resource.InstanceType,
			resource.Tag, resource.State, nullableID(resource.PriceQuoteID),
			nullableTime(resource.ProviderCreatedAt), resource.OpenedAt.UnixNano(),
			resource.UpdatedAt.UnixNano(), nullableTime(resource.ClosedAt), resource.CloseReason)
		if err != nil {
			return err
		}
		resource.ID, err = result.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mutations(
operation_id, kind, provider, tier, resource_id, state, created_at_ns)
VALUES (?, 'provision', ?, ?, ?, ?, ?)`, resource.OperationID, resource.Provider,
			resource.Tier, resource.ID, MutationPending, resource.OpenedAt.UnixNano())
		return err
	})
	return resource, err
}

// ActivateResource records a successful provider create and completes its
// provision intent atomically.
func (s *SQLite) ActivateResource(
	ctx context.Context,
	id int64,
	externalID, name string,
	providerCreatedAt, at time.Time,
) error {
	if err := require("external resource ID", externalID); err != nil {
		return err
	}
	at = normalizeTime(at)
	providerCreatedAt = normalizeTime(providerCreatedAt)
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE resources SET external_id = ?, name = ?,
state = ?, provider_created_at_ns = ?, updated_at_ns = ? WHERE id = ?`,
			externalID, name, ResourceActive, providerCreatedAt.UnixNano(), at.UnixNano(), id)
		if err != nil {
			return err
		}
		if err := requireAffected(result); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE mutations SET state = ?, external_id = ?,
completed_at_ns = ? WHERE operation_id = (SELECT operation_id FROM resources WHERE id = ?)`,
			MutationSucceeded, externalID, at.UnixNano(), id)
		return err
	})
}

// ActivateResourceWithGeneration atomically completes a provider-create
// intent and records the first filesystem generation. This prevents a crash
// between resource activation and generation insertion from making an
// unverified worker appear dispatchable during recovery.
func (s *SQLite) ActivateResourceWithGeneration(
	ctx context.Context,
	id int64,
	externalID, name string,
	providerCreatedAt, at time.Time,
	generation Generation,
) (Generation, error) {
	if err := require("external resource ID", externalID); err != nil {
		return Generation{}, err
	}
	if id == 0 {
		return Generation{}, errors.New("storage: resource ID is required")
	}
	if generation.ResourceID == 0 {
		generation.ResourceID = id
	}
	if generation.ResourceID != id {
		return Generation{}, errors.New("storage: generation resource ID does not match resource")
	}
	if err := require("operation ID", generation.OperationID); err != nil {
		return Generation{}, err
	}
	if generation.State == "" {
		generation.State = GenerationPreparing
	}
	at = normalizeTime(at)
	providerCreatedAt = normalizeTime(providerCreatedAt)
	generation.StartedAt = normalizeTime(generation.StartedAt)
	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE resources SET external_id = ?, name = ?,
state = ?, provider_created_at_ns = ?, updated_at_ns = ? WHERE id = ?`,
			externalID, name, ResourceActive, providerCreatedAt.UnixNano(), at.UnixNano(), id)
		if err != nil {
			return err
		}
		if err := requireAffected(result); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE mutations SET state = ?, external_id = ?,
completed_at_ns = ? WHERE operation_id = (SELECT operation_id FROM resources WHERE id = ?)`,
			MutationSucceeded, externalID, at.UnixNano(), id); err != nil {
			return err
		}
		if generation.Number == 0 {
			if err := tx.QueryRowContext(ctx,
				"SELECT COALESCE(MAX(generation_number), 0) + 1 FROM generations WHERE resource_id = ?",
				id).Scan(&generation.Number); err != nil {
				return err
			}
		}
		result, err = tx.ExecContext(ctx, `INSERT INTO generations(
resource_id, generation_number, operation_id, image_id, fingerprint, state,
started_at_ns, ready_at_ns, closed_at_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			generation.ResourceID, generation.Number, generation.OperationID,
			generation.ImageID, generation.Fingerprint, generation.State,
			generation.StartedAt.UnixNano(), nullableTime(generation.ReadyAt),
			nullableTime(generation.ClosedAt))
		if err != nil {
			return err
		}
		generation.ID, err = result.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mutations(
operation_id, kind, provider, tier, resource_id, state, created_at_ns)
SELECT ?, 'generation', provider, tier, id, ?, ? FROM resources WHERE id = ?`,
			generation.OperationID, MutationPending, generation.StartedAt.UnixNano(), id)
		return err
	})
	return generation, err
}

// SetResourceState records a non-terminal resource transition.
func (s *SQLite) SetResourceState(ctx context.Context, id int64, state ResourceState, at time.Time) error {
	if state == "" {
		return errors.New("storage: resource state is required")
	}
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx,
			"UPDATE resources SET state = ?, updated_at_ns = ? WHERE id = ?",
			state, at.UnixNano(), id)
		if err != nil {
			return err
		}
		return requireAffected(result)
	})
}

// CloseResource marks an allocation terminal. Historical generations, phases,
// jobs, and costs remain available until a retention pass.
func (s *SQLite) CloseResource(
	ctx context.Context,
	id int64,
	state ResourceState,
	reason string,
	at time.Time,
) error {
	if state != ResourceClosed && state != ResourceFailed && state != ResourceVanished {
		return fmt.Errorf("storage: terminal resource state required, got %q", state)
	}
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE resources SET state = ?, close_reason = ?,
updated_at_ns = ?, closed_at_ns = ? WHERE id = ?`, state, reason, at.UnixNano(), at.UnixNano(), id)
		if err != nil {
			return err
		}
		return requireAffected(result)
	})
}

const resourceColumns = `id, operation_id, provider, driver, tier, external_id,
name, instance_type, ownership_tag, state, price_quote_id, provider_created_at_ns,
opened_at_ns, updated_at_ns, closed_at_ns, close_reason`

// OpenResources returns allocations that require provider reconciliation.
func (s *SQLite) OpenResources(ctx context.Context) ([]Resource, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+resourceColumns+` FROM resources
WHERE closed_at_ns IS NULL AND state NOT IN (?, ?, ?) ORDER BY id`,
		ResourceClosed, ResourceFailed, ResourceVanished)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var resources []Resource
	for rows.Next() {
		resource, err := scanResource(rows)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, rows.Err()
}

func scanResource(scanner interface{ Scan(...any) error }) (Resource, error) {
	var resource Resource
	var externalID sql.NullString
	var quoteID, providerCreatedAt, closedAt sql.NullInt64
	var openedAt, updatedAt int64
	err := scanner.Scan(&resource.ID, &resource.OperationID, &resource.Provider,
		&resource.Driver, &resource.Tier, &externalID, &resource.Name,
		&resource.InstanceType, &resource.Tag, &resource.State, &quoteID,
		&providerCreatedAt, &openedAt, &updatedAt, &closedAt, &resource.CloseReason)
	resource.ExternalID = externalID.String
	resource.PriceQuoteID = quoteID.Int64
	resource.ProviderCreatedAt = timeFromNull(providerCreatedAt)
	resource.OpenedAt = time.Unix(0, openedAt).UTC()
	resource.UpdatedAt = time.Unix(0, updatedAt).UTC()
	resource.ClosedAt = timeFromNull(closedAt)
	return resource, err
}

// BeginGeneration records an in-place rebuild intent. Number is assigned
// monotonically per resource when left zero.
func (s *SQLite) BeginGeneration(ctx context.Context, generation Generation) (Generation, error) {
	if generation.ResourceID == 0 {
		return Generation{}, errors.New("storage: generation resource ID is required")
	}
	if err := require("operation ID", generation.OperationID); err != nil {
		return Generation{}, err
	}
	if generation.State == "" {
		generation.State = GenerationPreparing
	}
	generation.StartedAt = normalizeTime(generation.StartedAt)
	err := s.write(ctx, func(tx *sql.Tx) error {
		if generation.Number == 0 {
			if err := tx.QueryRowContext(ctx,
				"SELECT COALESCE(MAX(generation_number), 0) + 1 FROM generations WHERE resource_id = ?",
				generation.ResourceID).Scan(&generation.Number); err != nil {
				return err
			}
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO generations(
resource_id, generation_number, operation_id, image_id, fingerprint, state,
started_at_ns, ready_at_ns, closed_at_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			generation.ResourceID, generation.Number, generation.OperationID,
			generation.ImageID, generation.Fingerprint, generation.State,
			generation.StartedAt.UnixNano(), nullableTime(generation.ReadyAt),
			nullableTime(generation.ClosedAt))
		if err != nil {
			return err
		}
		generation.ID, err = result.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO mutations(
operation_id, kind, provider, tier, resource_id, state, created_at_ns)
SELECT ?, 'generation', provider, tier, id, ?, ? FROM resources WHERE id = ?`,
			generation.OperationID, MutationPending, generation.StartedAt.UnixNano(), generation.ResourceID)
		return err
	})
	return generation, err
}

// SetGenerationState updates recovery state and the corresponding timestamp.
func (s *SQLite) SetGenerationState(
	ctx context.Context,
	id int64,
	state GenerationState,
	at time.Time,
) error {
	switch state {
	case GenerationPreparing, GenerationReady, GenerationInUse, GenerationDirty, GenerationClosed, GenerationFailed:
	default:
		return fmt.Errorf("storage: unsupported generation state %q", state)
	}
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		var result sql.Result
		var err error
		switch state {
		case GenerationPreparing, GenerationInUse, GenerationDirty:
			result, err = tx.ExecContext(ctx,
				"UPDATE generations SET state = ? WHERE id = ?", state, id)
		case GenerationReady:
			result, err = tx.ExecContext(ctx,
				"UPDATE generations SET state = ?, ready_at_ns = ? WHERE id = ?",
				state, at.UnixNano(), id)
		case GenerationClosed, GenerationFailed:
			result, err = tx.ExecContext(ctx,
				"UPDATE generations SET state = ?, closed_at_ns = ? WHERE id = ?",
				state, at.UnixNano(), id)
		}
		if err != nil {
			return err
		}
		if err := requireAffected(result); err != nil {
			return err
		}
		switch state {
		case GenerationReady, GenerationClosed:
			_, err = tx.ExecContext(ctx, `UPDATE mutations SET state = ?, completed_at_ns = ?
WHERE operation_id = (SELECT operation_id FROM generations WHERE id = ?)`,
				MutationSucceeded, at.UnixNano(), id)
		case GenerationFailed:
			_, err = tx.ExecContext(ctx, `UPDATE mutations SET state = ?, completed_at_ns = ?
WHERE operation_id = (SELECT operation_id FROM generations WHERE id = ?)`,
				MutationFailed, at.UnixNano(), id)
		case GenerationPreparing, GenerationInUse, GenerationDirty:
		}
		return err
	})
}

// OpenGenerations returns generations requiring startup recovery.
func (s *SQLite) OpenGenerations(ctx context.Context) ([]Generation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, resource_id, generation_number,
operation_id, image_id, fingerprint, state, started_at_ns, ready_at_ns, closed_at_ns
FROM generations WHERE closed_at_ns IS NULL AND state NOT IN (?, ?) ORDER BY resource_id, generation_number`,
		GenerationClosed, GenerationFailed)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var generations []Generation
	for rows.Next() {
		generation, err := scanGeneration(rows)
		if err != nil {
			return nil, err
		}
		generations = append(generations, generation)
	}
	return generations, rows.Err()
}

func scanGeneration(scanner interface{ Scan(...any) error }) (Generation, error) {
	var generation Generation
	var startedAt int64
	var readyAt, closedAt sql.NullInt64
	err := scanner.Scan(&generation.ID, &generation.ResourceID, &generation.Number,
		&generation.OperationID, &generation.ImageID, &generation.Fingerprint,
		&generation.State, &startedAt, &readyAt, &closedAt)
	generation.StartedAt = time.Unix(0, startedAt).UTC()
	generation.ReadyAt = timeFromNull(readyAt)
	generation.ClosedAt = timeFromNull(closedAt)
	return generation, err
}

// StartPhase opens a lifecycle timing interval.
func (s *SQLite) StartPhase(ctx context.Context, phase Phase) (Phase, error) {
	if phase.ResourceID == 0 {
		return Phase{}, errors.New("storage: phase resource ID is required")
	}
	if phase.Kind == "" {
		return Phase{}, errors.New("storage: phase kind is required")
	}
	phase.StartedAt = normalizeTime(phase.StartedAt)
	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO phases(
resource_id, generation_id, job_id, kind, started_at_ns, ended_at_ns, outcome, detail)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, phase.ResourceID, nullableID(phase.GenerationID),
			nullableID(phase.JobID), phase.Kind, phase.StartedAt.UnixNano(),
			nullableTime(phase.EndedAt), phase.Outcome, phase.Detail)
		if err != nil {
			return err
		}
		phase.ID, err = result.LastInsertId()
		return err
	})
	return phase, err
}

// FinishPhase closes a lifecycle timing interval.
func (s *SQLite) FinishPhase(
	ctx context.Context,
	id int64,
	outcome, detail string,
	at time.Time,
) error {
	at = normalizeTime(at)
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE phases SET ended_at_ns = ?,
outcome = ?, detail = ? WHERE id = ?`, at.UnixNano(), outcome, detail, id)
		if err != nil {
			return err
		}
		return requireAffected(result)
	})
}

// OpenPhases returns intervals that were active when the daemon stopped.
func (s *SQLite) OpenPhases(ctx context.Context) ([]Phase, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, resource_id, generation_id,
job_id, kind, started_at_ns, ended_at_ns, outcome, detail
FROM phases WHERE ended_at_ns IS NULL ORDER BY started_at_ns, id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var phases []Phase
	for rows.Next() {
		phase, err := scanPhase(rows)
		if err != nil {
			return nil, err
		}
		phases = append(phases, phase)
	}
	return phases, rows.Err()
}

func scanPhase(scanner interface{ Scan(...any) error }) (Phase, error) {
	var phase Phase
	var generationID, jobID, endedAt sql.NullInt64
	var startedAt int64
	err := scanner.Scan(&phase.ID, &phase.ResourceID, &generationID, &jobID,
		&phase.Kind, &startedAt, &endedAt, &phase.Outcome, &phase.Detail)
	phase.GenerationID = generationID.Int64
	phase.JobID = jobID.Int64
	phase.StartedAt = time.Unix(0, startedAt).UTC()
	phase.EndedAt = timeFromNull(endedAt)
	return phase, err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func requireAffected(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}
