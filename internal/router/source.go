package router

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/storage"
)

// RoutedSource merges durable automatic assignments into one tier's ordinary
// Forgejo queue. All registration and runner operations remain delegated to
// the original source.
type RoutedSource struct {
	Base            orchestrator.JobSource
	Store           storage.RoutingStore
	Tier            string
	AutomaticLabels []string
	ExplicitLabels  []string
	Now             func() time.Time

	optimizationMu sync.RWMutex
	optimization   map[string]struct{}
}

var _ orchestrator.JobSource = (*RoutedSource)(nil)

// WaitingJobs returns explicit and routed work, deduplicated by queue handle.
func (s *RoutedSource) WaitingJobs(ctx context.Context) ([]forgejo.WaitingJob, error) {
	s.setOptimizationQueue(nil)
	jobs, err := s.Base.WaitingJobs(ctx)
	if err != nil {
		return nil, err
	}
	// A candidate runner advertises automatic route labels, but an
	// automatic-only job must not enter this tier until its durable decision
	// is visible below. Explicit tier labels retain precedence.
	filtered := jobs[:0]
	for _, job := range jobs {
		if hasExplicitLabel(job.Labels, s.AutomaticLabels) &&
			!hasExplicitLabel(job.Labels, s.ExplicitLabels) {
			continue
		}
		filtered = append(filtered, job)
	}
	jobs = filtered
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	routed, err := s.Store.PendingRoutedJobs(ctx, s.Tier, now)
	if err != nil {
		return nil, err
	}
	optimization := make(map[string]struct{})
	seen := make(map[string]struct{}, len(jobs)+len(routed))
	for _, job := range jobs {
		seen[job.Handle] = struct{}{}
	}
	for _, assignment := range routed {
		var job forgejo.WaitingJob
		if err := json.Unmarshal([]byte(assignment.PayloadJSON), &job); err != nil {
			return nil, err
		}
		if _, ok := seen[job.Handle]; ok {
			if assignment.OptimizationQueued {
				optimization[job.Handle] = struct{}{}
			}
			continue
		}
		seen[job.Handle] = struct{}{}
		jobs = append(jobs, job)
		if assignment.OptimizationQueued {
			optimization[job.Handle] = struct{}{}
		}
	}
	s.setOptimizationQueue(optimization)
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].ID == jobs[j].ID {
			return jobs[i].Handle < jobs[j].Handle
		}
		return jobs[i].ID < jobs[j].ID
	})
	return jobs, nil
}

// HoldForRoutingOptimization reports whether this exact waiting job was
// intentionally placed behind a paid worker. The orchestrator still dispatches
// it immediately when an idle worker exists; the flag only suppresses a new VM.
func (s *RoutedSource) HoldForRoutingOptimization(handle string) bool {
	s.optimizationMu.RLock()
	defer s.optimizationMu.RUnlock()
	_, ok := s.optimization[handle]
	return ok
}

func (s *RoutedSource) setOptimizationQueue(handles map[string]struct{}) {
	s.optimizationMu.Lock()
	s.optimization = handles
	s.optimizationMu.Unlock()
}

// RegisterEphemeral delegates one-shot registration to the tier's Forgejo source.
func (s *RoutedSource) RegisterEphemeral(ctx context.Context, name string, labels []string) (forgejo.Registration, error) {
	return s.Base.RegisterEphemeral(ctx, name, labels)
}

// ListRunners delegates runner discovery to the tier's Forgejo source.
func (s *RoutedSource) ListRunners(ctx context.Context) ([]forgejo.Runner, error) {
	return s.Base.ListRunners(ctx)
}

// DeleteRunner delegates stale registration cleanup to the tier's Forgejo source.
func (s *RoutedSource) DeleteRunner(ctx context.Context, id int64) error {
	return s.Base.DeleteRunner(ctx, id)
}

// JobMetadata preserves optional Forgejo enrichment through the wrapper.
func (s *RoutedSource) JobMetadata(ctx context.Context, job forgejo.WaitingJob) (forgejo.JobMetadata, error) {
	if source, ok := s.Base.(interface {
		JobMetadata(context.Context, forgejo.WaitingJob) (forgejo.JobMetadata, error)
	}); ok {
		return source.JobMetadata(ctx, job)
	}
	return forgejo.JobMetadata{}, forgejo.ErrMetadataUnavailable
}
