// Package mock provides a hand-written, concurrency-safe storage.Store fake.
package mock

import (
	"context"
	"sync"
	"time"

	"github.com/hstern/fj-bellows/internal/storage"
)

// Store is a configurable storage.Store. Unset functions return zero values;
// every invocation is recorded by method name for concurrency-safe assertions.
type Store struct {
	CloseFn              func() error
	HealthFn             func(context.Context) storage.Health
	BeginMutationFn      func(context.Context, storage.Mutation) error
	FinishMutationFn     func(context.Context, string, storage.MutationState, string, string, time.Time) error
	PendingMutationsFn   func(context.Context) ([]storage.Mutation, error)
	BeginResourceFn      func(context.Context, storage.Resource) (storage.Resource, error)
	ActivateResourceFn   func(context.Context, int64, string, string, time.Time, time.Time) error
	SetResourceStateFn   func(context.Context, int64, storage.ResourceState, time.Time) error
	CloseResourceFn      func(context.Context, int64, storage.ResourceState, string, time.Time) error
	OpenResourcesFn      func(context.Context) ([]storage.Resource, error)
	BeginGenerationFn    func(context.Context, storage.Generation) (storage.Generation, error)
	SetGenerationStateFn func(context.Context, int64, storage.GenerationState, time.Time) error
	OpenGenerationsFn    func(context.Context) ([]storage.Generation, error)
	StartPhaseFn         func(context.Context, storage.Phase) (storage.Phase, error)
	FinishPhaseFn        func(context.Context, int64, string, string, time.Time) error
	OpenPhasesFn         func(context.Context) ([]storage.Phase, error)
	UpsertJobFn          func(context.Context, storage.Job) (storage.Job, error)
	OpenJobsFn           func(context.Context) ([]storage.Job, error)
	JobHistoryFn         func(context.Context, storage.HistoryFilter) (storage.JobPage, error)
	BeginSnapshotFn      func(context.Context, storage.Snapshot) (storage.Snapshot, error)
	ActivateSnapshotFn   func(context.Context, int64, string, int64, time.Time) error
	SetSnapshotStateFn   func(context.Context, int64, storage.SnapshotState, string, time.Time) error
	SnapshotsFn          func(context.Context, storage.SnapshotFilter) ([]storage.Snapshot, error)
	RecordPriceQuoteFn   func(context.Context, storage.PriceQuote) (storage.PriceQuote, error)
	GetPriceQuoteFn      func(context.Context, int64) (storage.PriceQuote, error)
	LatestPriceQuoteFn   func(context.Context, string, string) (storage.PriceQuote, error)
	RecordCostFn         func(context.Context, storage.CostEntry) (storage.CostEntry, error)
	StatisticsFn         func(context.Context, storage.StatisticsFilter) (storage.Statistics, error)
	RetainFn             func(context.Context, time.Time) (storage.RetentionResult, error)

	mu    sync.Mutex
	calls []string
}

var _ storage.Store = (*Store)(nil)

func (s *Store) record(method string) {
	s.mu.Lock()
	s.calls = append(s.calls, method)
	s.mu.Unlock()
}

// Calls returns a copy of method names in invocation order.
func (s *Store) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// CallCount returns the number of calls to method.
func (s *Store) CallCount(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, call := range s.calls {
		if call == method {
			count++
		}
	}
	return count
}

// Close implements storage.Store.
func (s *Store) Close() error {
	s.record("Close")
	if s.CloseFn != nil {
		return s.CloseFn()
	}
	return nil
}

// Health implements storage.Store.
func (s *Store) Health(ctx context.Context) storage.Health {
	s.record("Health")
	if s.HealthFn != nil {
		return s.HealthFn(ctx)
	}
	return storage.Health{Healthy: true}
}

// BeginMutation implements storage.Store.
func (s *Store) BeginMutation(ctx context.Context, value storage.Mutation) error {
	s.record("BeginMutation")
	if s.BeginMutationFn != nil {
		return s.BeginMutationFn(ctx, value)
	}
	return nil
}

// FinishMutation implements storage.Store.
func (s *Store) FinishMutation(ctx context.Context, id string, state storage.MutationState, externalID, detail string, at time.Time) error {
	s.record("FinishMutation")
	if s.FinishMutationFn != nil {
		return s.FinishMutationFn(ctx, id, state, externalID, detail, at)
	}
	return nil
}

// PendingMutations implements storage.Store.
func (s *Store) PendingMutations(ctx context.Context) ([]storage.Mutation, error) {
	s.record("PendingMutations")
	if s.PendingMutationsFn != nil {
		return s.PendingMutationsFn(ctx)
	}
	return nil, nil
}

// BeginResource implements storage.Store.
func (s *Store) BeginResource(ctx context.Context, value storage.Resource) (storage.Resource, error) {
	s.record("BeginResource")
	if s.BeginResourceFn != nil {
		return s.BeginResourceFn(ctx, value)
	}
	return value, nil
}

// ActivateResource implements storage.Store.
func (s *Store) ActivateResource(ctx context.Context, id int64, externalID, name string, createdAt, at time.Time) error {
	s.record("ActivateResource")
	if s.ActivateResourceFn != nil {
		return s.ActivateResourceFn(ctx, id, externalID, name, createdAt, at)
	}
	return nil
}

// SetResourceState implements storage.Store.
func (s *Store) SetResourceState(ctx context.Context, id int64, state storage.ResourceState, at time.Time) error {
	s.record("SetResourceState")
	if s.SetResourceStateFn != nil {
		return s.SetResourceStateFn(ctx, id, state, at)
	}
	return nil
}

// CloseResource implements storage.Store.
func (s *Store) CloseResource(ctx context.Context, id int64, state storage.ResourceState, detail string, at time.Time) error {
	s.record("CloseResource")
	if s.CloseResourceFn != nil {
		return s.CloseResourceFn(ctx, id, state, detail, at)
	}
	return nil
}

// OpenResources implements storage.Store.
func (s *Store) OpenResources(ctx context.Context) ([]storage.Resource, error) {
	s.record("OpenResources")
	if s.OpenResourcesFn != nil {
		return s.OpenResourcesFn(ctx)
	}
	return nil, nil
}

// BeginGeneration implements storage.Store.
func (s *Store) BeginGeneration(ctx context.Context, value storage.Generation) (storage.Generation, error) {
	s.record("BeginGeneration")
	if s.BeginGenerationFn != nil {
		return s.BeginGenerationFn(ctx, value)
	}
	return value, nil
}

// SetGenerationState implements storage.Store.
func (s *Store) SetGenerationState(ctx context.Context, id int64, state storage.GenerationState, at time.Time) error {
	s.record("SetGenerationState")
	if s.SetGenerationStateFn != nil {
		return s.SetGenerationStateFn(ctx, id, state, at)
	}
	return nil
}

// OpenGenerations implements storage.Store.
func (s *Store) OpenGenerations(ctx context.Context) ([]storage.Generation, error) {
	s.record("OpenGenerations")
	if s.OpenGenerationsFn != nil {
		return s.OpenGenerationsFn(ctx)
	}
	return nil, nil
}

// StartPhase implements storage.Store.
func (s *Store) StartPhase(ctx context.Context, value storage.Phase) (storage.Phase, error) {
	s.record("StartPhase")
	if s.StartPhaseFn != nil {
		return s.StartPhaseFn(ctx, value)
	}
	return value, nil
}

// FinishPhase implements storage.Store.
func (s *Store) FinishPhase(ctx context.Context, id int64, outcome, detail string, at time.Time) error {
	s.record("FinishPhase")
	if s.FinishPhaseFn != nil {
		return s.FinishPhaseFn(ctx, id, outcome, detail, at)
	}
	return nil
}

// OpenPhases implements storage.Store.
func (s *Store) OpenPhases(ctx context.Context) ([]storage.Phase, error) {
	s.record("OpenPhases")
	if s.OpenPhasesFn != nil {
		return s.OpenPhasesFn(ctx)
	}
	return nil, nil
}

// UpsertJob implements storage.Store.
func (s *Store) UpsertJob(ctx context.Context, value storage.Job) (storage.Job, error) {
	s.record("UpsertJob")
	if s.UpsertJobFn != nil {
		return s.UpsertJobFn(ctx, value)
	}
	return value, nil
}

// OpenJobs implements storage.Store.
func (s *Store) OpenJobs(ctx context.Context) ([]storage.Job, error) {
	s.record("OpenJobs")
	if s.OpenJobsFn != nil {
		return s.OpenJobsFn(ctx)
	}
	return nil, nil
}

// JobHistory implements storage.Store.
func (s *Store) JobHistory(ctx context.Context, filter storage.HistoryFilter) (storage.JobPage, error) {
	s.record("JobHistory")
	if s.JobHistoryFn != nil {
		return s.JobHistoryFn(ctx, filter)
	}
	return storage.JobPage{}, nil
}

// BeginSnapshot implements storage.Store.
func (s *Store) BeginSnapshot(ctx context.Context, value storage.Snapshot) (storage.Snapshot, error) {
	s.record("BeginSnapshot")
	if s.BeginSnapshotFn != nil {
		return s.BeginSnapshotFn(ctx, value)
	}
	return value, nil
}

// ActivateSnapshot implements storage.Store.
func (s *Store) ActivateSnapshot(ctx context.Context, id int64, externalID string, size int64, at time.Time) error {
	s.record("ActivateSnapshot")
	if s.ActivateSnapshotFn != nil {
		return s.ActivateSnapshotFn(ctx, id, externalID, size, at)
	}
	return nil
}

// SetSnapshotState implements storage.Store.
func (s *Store) SetSnapshotState(ctx context.Context, id int64, state storage.SnapshotState, detail string, at time.Time) error {
	s.record("SetSnapshotState")
	if s.SetSnapshotStateFn != nil {
		return s.SetSnapshotStateFn(ctx, id, state, detail, at)
	}
	return nil
}

// Snapshots implements storage.Store.
func (s *Store) Snapshots(ctx context.Context, filter storage.SnapshotFilter) ([]storage.Snapshot, error) {
	s.record("Snapshots")
	if s.SnapshotsFn != nil {
		return s.SnapshotsFn(ctx, filter)
	}
	return nil, nil
}

// RecordPriceQuote implements storage.Store.
func (s *Store) RecordPriceQuote(ctx context.Context, value storage.PriceQuote) (storage.PriceQuote, error) {
	s.record("RecordPriceQuote")
	if s.RecordPriceQuoteFn != nil {
		return s.RecordPriceQuoteFn(ctx, value)
	}
	return value, nil
}

// GetPriceQuote implements storage.Store.
func (s *Store) GetPriceQuote(ctx context.Context, id int64) (storage.PriceQuote, error) {
	s.record("GetPriceQuote")
	if s.GetPriceQuoteFn != nil {
		return s.GetPriceQuoteFn(ctx, id)
	}
	return storage.PriceQuote{}, nil
}

// LatestPriceQuote implements storage.Store.
func (s *Store) LatestPriceQuote(ctx context.Context, provider, instanceType string) (storage.PriceQuote, error) {
	s.record("LatestPriceQuote")
	if s.LatestPriceQuoteFn != nil {
		return s.LatestPriceQuoteFn(ctx, provider, instanceType)
	}
	return storage.PriceQuote{}, nil
}

// RecordCost implements storage.Store.
func (s *Store) RecordCost(ctx context.Context, value storage.CostEntry) (storage.CostEntry, error) {
	s.record("RecordCost")
	if s.RecordCostFn != nil {
		return s.RecordCostFn(ctx, value)
	}
	return value, nil
}

// Statistics implements storage.Store.
func (s *Store) Statistics(ctx context.Context, filter storage.StatisticsFilter) (storage.Statistics, error) {
	s.record("Statistics")
	if s.StatisticsFn != nil {
		return s.StatisticsFn(ctx, filter)
	}
	return storage.Statistics{}, nil
}

// Retain implements storage.Store.
func (s *Store) Retain(ctx context.Context, before time.Time) (storage.RetentionResult, error) {
	s.record("Retain")
	if s.RetainFn != nil {
		return s.RetainFn(ctx, before)
	}
	return storage.RetentionResult{}, nil
}
