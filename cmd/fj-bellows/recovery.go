package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/hstern/fj-bellows/internal/storage"
)

// recoverInterruptedJobs makes restart classification replay-safe. The clean
// generation and cost are persisted before the terminal job update: if the
// process dies between any two writes, the still-open job is retried and the
// terminal-aware cost key resolves to the original immutable row.
func recoverInterruptedJobs(ctx context.Context, store storage.Store, now time.Time) error {
	jobs, err := store.OpenJobs(ctx)
	if err != nil {
		return fmt.Errorf("list open jobs: %w", err)
	}
	resources, err := store.OpenResources(ctx)
	if err != nil {
		return fmt.Errorf("list open resources for interrupted jobs: %w", err)
	}
	resourceByID := make(map[int64]storage.Resource, len(resources))
	for _, resource := range resources {
		resourceByID[resource.ID] = resource
	}

	for _, job := range jobs {
		if job.Status != storage.JobAssigned && job.Status != storage.JobRunning {
			continue
		}
		if job.GenerationID != 0 {
			if err := store.SetGenerationState(ctx, job.GenerationID, storage.GenerationDirty, now); err != nil {
				return fmt.Errorf("mark interrupted generation %d dirty: %w", job.GenerationID, err)
			}
		}
		if err := recordInterruptedDirectCost(ctx, store, resourceByID[job.ResourceID], job, now); err != nil {
			return fmt.Errorf("price interrupted job %s: %w", job.Handle, err)
		}
		job.Status = storage.JobInterrupted
		job.InfrastructureFailure = "daemon_restart"
		job.RunnerFinishedAt = now
		job.CompletedAt = now
		job.UpdatedAt = now
		if _, err := store.UpsertJob(ctx, job); err != nil {
			return fmt.Errorf("interrupt job %s: %w", job.Handle, err)
		}
	}
	return nil
}

func recordInterruptedDirectCost(
	ctx context.Context,
	store storage.Store,
	resource storage.Resource,
	job storage.Job,
	end time.Time,
) error {
	start := job.RunnerStartedAt
	if start.IsZero() || start.After(end) {
		start = job.DispatchedAt
	}
	if start.IsZero() || start.After(end) {
		start = job.FirstSeenAt
	}
	if start.IsZero() || start.After(end) {
		start = end
	}
	entry := storage.CostEntry{
		ResourceID: job.ResourceID, JobID: job.ID, PriceQuoteID: resource.PriceQuoteID,
		Kind: storage.CostDirectCompute, Estimated: true,
		StartedAt: start, EndedAt: end, RecordedAt: end,
	}
	if resource.PriceQuoteID != 0 {
		quote, err := store.GetPriceQuote(ctx, resource.PriceQuoteID)
		if err == nil {
			entry.Known = true
			entry.Currency = quote.Currency
			entry.Nanos = recoveryProportionalNanos(quote.PerHourNanos, end.Sub(start), time.Hour)
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
	}
	_, err := store.RecordCost(ctx, entry)
	return err
}

func recoveryProportionalNanos(rate int64, elapsed, period time.Duration) int64 {
	if rate <= 0 || elapsed <= 0 || period <= 0 {
		return 0
	}
	value := new(big.Int).Mul(big.NewInt(rate), big.NewInt(int64(elapsed)))
	value.Quo(value, big.NewInt(int64(period)))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}
