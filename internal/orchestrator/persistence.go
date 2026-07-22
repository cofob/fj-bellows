package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

func (o *Orchestrator) storageError(operation string, err error) {
	if err == nil {
		o.storageFailed.Store(false)
		return
	}
	o.storageFailed.Store(true)
	o.log.Error("durable storage", "operation", operation, "err", err)
	o.emit("storage_failed", map[string]string{"operation": operation})
}

func (o *Orchestrator) persistObservedJobs(ctx context.Context, jobs []forgejo.WaitingJob) error {
	if o.store == nil {
		return nil
	}
	now := o.now()
	for i := range jobs {
		if jobs[i].Handle == "" {
			jobs[i].Handle = strconv.FormatInt(jobs[i].ID, 10)
		}
		quality := storage.IdentityFallback
		workflow := jobs[i].Name
		if jobs[i].WorkflowID != "" {
			quality = storage.IdentityExact
			workflow = jobs[i].WorkflowID
		}
		_, err := o.store.UpsertJob(ctx, storage.Job{
			Source: o.cfg.ForgejoSource, Handle: jobs[i].Handle,
			ForgejoJobID: jobs[i].ID, Attempt: jobs[i].Attempt,
			RepositoryID: jobs[i].RepoID, RepositoryOwnerID: jobs[i].OwnerID,
			WorkflowFile: workflow, JobName: jobs[i].Name, IdentityQuality: quality,
			Tier: o.cfg.Tier, Provider: o.cfg.ProviderName, Driver: o.cfg.Driver,
			Status: storage.JobObserved, QueueMeasurementSource: "first_observed",
			FirstSeenAt: now, QueuedAt: now, UpdatedAt: now,
		})
		if err != nil {
			o.storageError("observe job", err)
			return err
		}
	}
	o.storageError("observe jobs", nil)
	return nil
}

func (o *Orchestrator) persistJob(ctx context.Context, node Node, job forgejo.WaitingJob, status storage.JobStatus, failure string, at time.Time) (storage.Job, error) {
	if o.store == nil {
		return storage.Job{}, nil
	}
	record := storage.Job{
		Source: o.cfg.ForgejoSource, Handle: job.Handle,
		ForgejoJobID: job.ID, Attempt: job.Attempt,
		RepositoryID: job.RepoID, RepositoryOwnerID: job.OwnerID,
		JobName: job.Name, Tier: o.cfg.Tier, Provider: o.cfg.ProviderName,
		Driver: o.cfg.Driver, ResourceID: node.ResourceID,
		GenerationID: node.GenerationID, Status: status,
		InfrastructureFailure: failure, UpdatedAt: at,
	}
	switch status {
	case storage.JobObserved:
		// Observation does not establish a dispatch or completion timestamp.
	case storage.JobAssigned:
		record.DispatchedAt = at
	case storage.JobRunning:
		record.RunnerStartedAt = at
	case storage.JobSucceeded, storage.JobFailed, storage.JobCancelled,
		storage.JobInfraFailed, storage.JobInterrupted, storage.JobSkipped:
		record.RunnerFinishedAt = at
		record.CompletedAt = at
	}
	result, err := o.store.UpsertJob(ctx, record)
	o.storageError("update job", err)
	return result, err
}

func (o *Orchestrator) enrichJob(ctx context.Context, job forgejo.WaitingJob, status storage.JobStatus) {
	if o.store == nil {
		return
	}
	enricher, ok := o.jobs.(interface {
		JobMetadata(context.Context, forgejo.WaitingJob) (forgejo.JobMetadata, error)
	})
	if !ok {
		return
	}
	meta, err := enricher.JobMetadata(ctx, job)
	if err != nil {
		if !errors.Is(err, forgejo.ErrMetadataUnavailable) {
			o.log.Debug("enrich job metadata", "handle", job.Handle, "err", err)
		}
		return
	}
	quality := storage.IdentityFallback
	workflow := job.Name
	if meta.WorkflowID != "" {
		quality = storage.IdentityExact
		workflow = meta.WorkflowID
	}
	if mapped := conclusionStatus(meta.Conclusion); mapped != "" {
		status = mapped
	}
	now := o.now()
	record := storage.Job{
		Source: o.cfg.ForgejoSource, Handle: job.Handle, Repository: meta.Repository,
		RepositoryID: job.RepoID, WorkflowFile: workflow, JobName: job.Name,
		IdentityQuality: quality, Conclusion: meta.Conclusion, Status: status,
		RunMeasurementSource: "forgejo_api", UpdatedAt: now,
	}
	record.QueuedAt = parseForgejoTime(meta.QueuedAt)
	record.RunnerStartedAt = parseForgejoTime(meta.StartedAt)
	record.RunnerFinishedAt = parseForgejoTime(meta.CompletedAt)
	if !record.RunnerFinishedAt.IsZero() {
		record.CompletedAt = record.RunnerFinishedAt
	}
	if _, err := o.store.UpsertJob(ctx, record); err != nil {
		o.storageError("enrich job", err)
	}
}

func conclusionStatus(conclusion string) storage.JobStatus {
	switch strings.ToLower(conclusion) {
	case "success":
		return storage.JobSucceeded
	case "failure":
		return storage.JobFailed
	case "cancelled", "canceled":
		return storage.JobCancelled
	case "skipped":
		return storage.JobSkipped
	default:
		return ""
	}
}

func parseForgejoTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if value, err := time.Parse(layout, raw); err == nil {
			return value.UTC()
		}
	}
	return time.Time{}
}

func (o *Orchestrator) beginStoredResource(ctx context.Context, operationID, name string) (storage.Resource, storage.Phase, error) {
	if o.store == nil {
		return storage.Resource{}, storage.Phase{}, nil
	}
	quoteID := int64(0)
	if pricer, ok := o.prov.(provider.Pricer); ok {
		if quote, err := pricer.Quote(ctx, o.cfg.InstanceType); err == nil {
			stored, serr := o.store.RecordPriceQuote(ctx, storage.PriceQuote{
				Provider: o.cfg.ProviderName, Driver: o.cfg.Driver,
				InstanceType: quote.InstanceType, Currency: quote.Currency,
				PerHourNanos: quote.PerHourNanos, PerMonthNanos: quote.PerMonthNanos,
				SnapshotGBMonthNanos: quote.SnapshotGBMonthNanos,
				BillingQuantum:       quote.BillingQuantum, MinimumDuration: quote.MinimumDuration,
				MinimumChargeNanos: quote.MinimumChargeNanos,
				Source:             quote.Source, ObservedAt: quote.ObservedAt,
			})
			if serr != nil {
				o.storageError("record price quote", serr)
				return storage.Resource{}, storage.Phase{}, serr
			}
			quoteID = stored.ID
		}
	}
	resource, err := o.store.BeginResource(ctx, storage.Resource{
		OperationID: operationID, Provider: o.cfg.ProviderName, Driver: o.cfg.Driver,
		Tier: o.cfg.Tier, Name: name, InstanceType: o.cfg.InstanceType,
		Tag: o.cfg.Tag, State: storage.ResourceProvisioning,
		PriceQuoteID: quoteID, OpenedAt: o.now(),
	})
	o.storageError("begin resource", err)
	if err != nil {
		return storage.Resource{}, storage.Phase{}, err
	}
	phase, err := o.store.StartPhase(ctx, storage.Phase{
		ResourceID: resource.ID, Kind: storage.PhaseProvisioning, StartedAt: resource.OpenedAt,
	})
	o.storageError("begin provisioning phase", err)
	return resource, phase, err
}

func (o *Orchestrator) activateStoredResource(ctx context.Context, resource storage.Resource, inst provider.Instance) (storage.Generation, error) {
	if o.store == nil || resource.ID == 0 {
		return storage.Generation{}, nil
	}
	now := o.now()
	generation := storage.Generation{
		ResourceID: resource.ID, OperationID: resource.OperationID + "-generation-1",
		ImageID: o.managedImageID(), Fingerprint: o.imageFingerprint(),
		State: storage.GenerationPreparing, StartedAt: now,
	}
	if activator, ok := o.store.(storage.ResourceGenerationActivator); ok {
		generation, err := activator.ActivateResourceWithGeneration(
			ctx, resource.ID, inst.ID, inst.Name, inst.CreatedAt, now, generation,
		)
		o.storageError("activate resource generation", err)
		return generation, err
	}
	if err := o.store.ActivateResource(ctx, resource.ID, inst.ID, inst.Name, inst.CreatedAt, o.now()); err != nil {
		o.storageError("activate resource", err)
		return storage.Generation{}, err
	}
	generation, err := o.store.BeginGeneration(ctx, generation)
	o.storageError("begin generation", err)
	return generation, err
}

func (o *Orchestrator) closeStoredResource(ctx context.Context, node Node, state storage.ResourceState, reason string) {
	if o.store == nil || node.ResourceID == 0 {
		return
	}
	if node.GenerationID != 0 {
		_ = o.store.SetGenerationState(ctx, node.GenerationID, storage.GenerationClosed, o.now())
	}
	o.recordResourceCost(ctx, node, storage.CostBilledCompute, o.now())
	if err := o.store.CloseResource(ctx, node.ResourceID, state, reason, o.now()); err != nil {
		o.storageError("close resource", err)
	}
}

func (o *Orchestrator) failStoredProvision(ctx context.Context, resource storage.Resource, phaseID int64, detail string) {
	if o.store == nil || resource.ID == 0 {
		return
	}
	if phaseID != 0 {
		_ = o.store.FinishPhase(ctx, phaseID, "failed", detail, o.now())
	}
	_ = o.store.FinishMutation(ctx, resource.OperationID, storage.MutationFailed, "", detail, o.now())
	if err := o.store.CloseResource(ctx, resource.ID, storage.ResourceFailed, detail, o.now()); err != nil {
		o.storageError("fail resource", err)
	}
}

// cleanupFailedActivation compensates a provider create when its durable
// activation could not be committed. If deletion also fails, the provisioning
// row is intentionally left open so name/tag recovery can adopt and drain the
// still-existing VM on the next reconcile.
func (o *Orchestrator) cleanupFailedActivation(
	ctx context.Context,
	resource storage.Resource,
	phaseID int64,
	inst provider.Instance,
	detail string,
) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	if err := o.prov.Destroy(cleanupCtx, inst.ID); err != nil {
		o.finishStoredPhase(cleanupCtx, phaseID, "failed", detail+": "+err.Error(), o.now())
		o.log.Error("compensate failed resource activation", "id", inst.ID, "err", err)
		return
	}
	node := Node{
		InstanceID: inst.ID, ResourceID: resource.ID, PriceQuoteID: resource.PriceQuoteID,
		CreatedAt: inst.CreatedAt,
	}
	o.recordResourceCost(cleanupCtx, node, storage.CostBilledCompute, o.now())
	o.failStoredProvision(cleanupCtx, resource, phaseID, detail)
}

func (o *Orchestrator) beginStoredMutation(ctx context.Context, kind string, node Node) (string, error) {
	if o.store == nil {
		return "", nil
	}
	op := fmt.Sprintf("%s-%s-%s", kind, node.InstanceID, shortID())
	err := o.store.BeginMutation(ctx, storage.Mutation{
		OperationID: op, Kind: kind, Provider: o.cfg.ProviderName, Tier: o.cfg.Tier,
		ResourceID: node.ResourceID, State: storage.MutationPending, CreatedAt: o.now(),
	})
	if err == nil && node.ResourceID != 0 && strings.Contains(kind, "destroy") {
		err = o.store.SetResourceState(ctx, node.ResourceID, storage.ResourceDestroying, o.now())
	}
	o.storageError("begin "+kind, err)
	return op, err
}

func (o *Orchestrator) recordDirectJobCost(ctx context.Context, node Node, job storage.Job) {
	if o.store == nil || job.ID == 0 {
		return
	}
	start, end := job.RunnerStartedAt, job.RunnerFinishedAt
	if start.IsZero() || end.IsZero() || end.Before(start) {
		start, end = job.DispatchedAt, job.CompletedAt
	}
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return
	}
	entry := storage.CostEntry{
		ResourceID: node.ResourceID, JobID: job.ID, PriceQuoteID: node.PriceQuoteID,
		Kind: storage.CostDirectCompute, Estimated: true, StartedAt: start, EndedAt: end,
		RecordedAt: o.now(),
	}
	quote, err := o.store.GetPriceQuote(ctx, node.PriceQuoteID)
	if err == nil && node.PriceQuoteID != 0 {
		entry.Known = true
		entry.Currency = quote.Currency
		entry.Nanos = proportionalNanos(quote.PerHourNanos, end.Sub(start), time.Hour)
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		o.storageError("read job price quote", err)
		return
	}
	if _, err := o.store.RecordCost(ctx, entry); err != nil {
		o.storageError("record direct job cost", err)
	}
}

func (o *Orchestrator) recordIntervalCost(ctx context.Context, node Node, kind storage.CostKind, start, end time.Time) {
	if o.store == nil || node.ResourceID == 0 || start.IsZero() || end.Before(start) {
		return
	}
	entry := storage.CostEntry{
		ResourceID: node.ResourceID, PriceQuoteID: node.PriceQuoteID, Kind: kind,
		Estimated: true, StartedAt: start, EndedAt: end, RecordedAt: o.now(),
	}
	quote, err := o.store.GetPriceQuote(ctx, node.PriceQuoteID)
	if err == nil && node.PriceQuoteID != 0 {
		entry.Known = true
		entry.Currency = quote.Currency
		entry.Nanos = proportionalNanos(quote.PerHourNanos, end.Sub(start), time.Hour)
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		o.storageError("read interval price quote", err)
		return
	}
	if _, err := o.store.RecordCost(ctx, entry); err != nil {
		o.storageError("record "+string(kind)+" cost", err)
	}
}

func (o *Orchestrator) recordResourceCost(ctx context.Context, node Node, kind storage.CostKind, end time.Time) {
	if o.store == nil || node.ResourceID == 0 || node.CreatedAt.IsZero() || end.Before(node.CreatedAt) {
		return
	}
	entry := storage.CostEntry{
		ResourceID: node.ResourceID, PriceQuoteID: node.PriceQuoteID, Kind: kind,
		Estimated: true, StartedAt: node.CreatedAt, EndedAt: end, RecordedAt: o.now(),
	}
	quote, err := o.store.GetPriceQuote(ctx, node.PriceQuoteID)
	if err == nil && node.PriceQuoteID != 0 {
		entry.Known = true
		entry.Currency = quote.Currency
		entry.Nanos = resourceBilledNanos(quote, node.CreatedAt, end)
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		o.storageError("read resource price quote", err)
		return
	}
	if _, err := o.store.RecordCost(ctx, entry); err != nil {
		o.storageError("record resource cost", err)
		return
	}
	if !entry.Known || kind != storage.CostBilledCompute {
		return
	}
	linear := proportionalNanos(quote.PerHourNanos, end.Sub(node.CreatedAt), time.Hour)
	if overhead := entry.Nanos - linear; overhead > 0 {
		_, err := o.store.RecordCost(ctx, storage.CostEntry{
			ResourceID: node.ResourceID, PriceQuoteID: node.PriceQuoteID,
			Kind: storage.CostBillingOverhead, Currency: quote.Currency, Nanos: overhead,
			Known: true, Estimated: true, StartedAt: node.CreatedAt, EndedAt: end,
			RecordedAt: o.now(),
		})
		if err != nil {
			o.storageError("record billing overhead", err)
		}
	}
}

// resourceBilledNanos applies provider rounding once to the allocation, then
// applies a monthly maximum independently to every UTC calendar billing month
// touched by that rounded interval. A long-lived VM therefore accrues another
// cap in the next month instead of being capped at one month's price forever.
func resourceBilledNanos(quote storage.PriceQuote, start, end time.Time) int64 {
	billed := roundedBillingDuration(end.Sub(start), quote.BillingQuantum, quote.MinimumDuration)
	nanos, months := monthlyCappedNanos(quote.PerHourNanos, quote.PerMonthNanos, start, start.Add(billed))
	if nanos < quote.MinimumChargeNanos {
		nanos = quote.MinimumChargeNanos
	}
	if quote.PerMonthNanos <= 0 {
		return nanos
	}
	if months == 0 {
		months = 1
	}
	maximum := saturatedNanosProduct(quote.PerMonthNanos, months)
	if nanos > maximum {
		return maximum
	}
	return nanos
}

func monthlyCappedNanos(rate, monthlyCap int64, start, end time.Time) (int64, int64) {
	if rate <= 0 || !end.After(start) {
		return 0, 0
	}
	if monthlyCap <= 0 {
		return proportionalNanos(rate, end.Sub(start), time.Hour), 0
	}
	start, end = start.UTC(), end.UTC()
	var total, months int64
	for cursor := start; cursor.Before(end); {
		boundary := time.Date(cursor.Year(), cursor.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		segmentEnd := end
		if boundary.Before(end) {
			segmentEnd = boundary
		}
		segment := proportionalNanos(rate, segmentEnd.Sub(cursor), time.Hour)
		segment = min(segment, monthlyCap)
		if total > math.MaxInt64-segment {
			return math.MaxInt64, months + 1
		}
		total += segment
		months++
		cursor = segmentEnd
	}
	return total, months
}

func saturatedNanosProduct(value, count int64) int64 {
	if value <= 0 || count <= 0 {
		return 0
	}
	if value > math.MaxInt64/count {
		return math.MaxInt64
	}
	return value * count
}

func roundedBillingDuration(elapsed, quantum, minimum time.Duration) time.Duration {
	if elapsed < 0 {
		return 0
	}
	if quantum > 0 && elapsed%quantum != 0 {
		elapsed += quantum - elapsed%quantum
	}
	if elapsed < minimum {
		elapsed = minimum
	}
	return elapsed
}

func proportionalNanos(rate int64, elapsed, period time.Duration) int64 {
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

func (o *Orchestrator) finishStoredMutation(ctx context.Context, operationID, externalID, detail string, success bool) {
	if o.store == nil || operationID == "" {
		return
	}
	state := storage.MutationSucceeded
	if !success {
		state = storage.MutationFailed
	}
	if err := o.store.FinishMutation(ctx, operationID, state, externalID, detail, o.now()); err != nil {
		o.storageError("finish mutation", err)
	}
}

func (o *Orchestrator) storageResources(ctx context.Context) (map[string]storage.Resource, map[int64]storage.Generation, error) {
	resources := map[string]storage.Resource{}
	generations := map[int64]storage.Generation{}
	if o.store == nil {
		return resources, generations, nil
	}
	open, err := o.store.OpenResources(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, resource := range open {
		if resource.Provider == o.cfg.ProviderName && resource.Tier == o.cfg.Tier {
			if resource.ExternalID != "" {
				resources[resource.ExternalID] = resource
			}
			if resource.Name != "" {
				resources["name:"+resource.Name] = resource
			}
		}
	}
	openGenerations, err := o.store.OpenGenerations(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, generation := range openGenerations {
		if current, ok := generations[generation.ResourceID]; !ok || generation.Number > current.Number {
			generations[generation.ResourceID] = generation
		}
	}
	return resources, generations, nil
}

//nolint:gocyclo // Mutation recovery explicitly classifies each intent kind against provider truth.
func (o *Orchestrator) recoverPendingMutations(
	ctx context.Context,
	seen map[string]struct{},
	resources map[string]storage.Resource,
	generations map[int64]storage.Generation,
) error {
	if o.store == nil {
		return nil
	}
	pending, err := o.store.PendingMutations(ctx)
	if err != nil {
		return err
	}
	byID := make(map[int64]storage.Resource)
	for _, resource := range resources {
		if resource.ID != 0 {
			byID[resource.ID] = resource
		}
	}
	for _, mutation := range pending {
		if mutation.Provider != o.cfg.ProviderName || mutation.Tier != o.cfg.Tier {
			continue
		}
		resource := byID[mutation.ResourceID]
		switch mutation.Kind {
		case "provision":
			if resource.ExternalID != "" {
				if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationSucceeded,
					resource.ExternalID, "recovered", o.now()); err != nil {
					return err
				}
				continue
			}
			if o.pendingCount() != 0 {
				continue
			}
			if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationFailed,
				"", "no provider resource found during recovery", o.now()); err != nil {
				return err
			}
			if resource.ID != 0 {
				if err := o.store.CloseResource(ctx, resource.ID, storage.ResourceFailed,
					"provision_not_found", o.now()); err != nil {
					return err
				}
			}
		case "generation":
			generation := generations[resource.ID]
			if generation.ID == 0 {
				if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationFailed,
					"", "generation record missing during recovery", o.now()); err != nil {
					return err
				}
				continue
			}
			if generation.State == storage.GenerationReady || generation.State == storage.GenerationClosed {
				if err := o.store.FinishMutation(ctx, mutation.OperationID, storage.MutationSucceeded,
					resource.ExternalID, "recovered", o.now()); err != nil {
					return err
				}
				continue
			}
			if err := o.store.SetGenerationState(ctx, generation.ID, storage.GenerationFailed, o.now()); err != nil {
				return err
			}
		default:
			if !strings.Contains(mutation.Kind, "destroy") || mutation.Kind == mutationDeleteSnapshot {
				continue
			}
			_, exists := seen[resource.ExternalID]
			state := storage.MutationFailed
			detail := "provider resource still exists; queued for retry"
			if resource.ExternalID == "" || !exists {
				state = storage.MutationSucceeded
				detail = "provider resource absent during recovery"
			}
			if err := o.store.FinishMutation(ctx, mutation.OperationID, state,
				resource.ExternalID, detail, o.now()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Orchestrator) adoptStoredResource(ctx context.Context, inst provider.Instance) (storage.Resource, storage.Generation, error) {
	op := fmt.Sprintf("adopt-%s-%s-%s", o.cfg.Tier, inst.ID, shortID())
	resource, phase, err := o.beginStoredResource(ctx, op, inst.Name)
	if err != nil {
		return storage.Resource{}, storage.Generation{}, err
	}
	generation, err := o.activateStoredResource(ctx, resource, inst)
	if err == nil && phase.ID != 0 {
		_ = o.store.FinishPhase(ctx, phase.ID, "adopted", "", o.now())
	}
	return resource, generation, err
}

func (o *Orchestrator) finishStoredPhase(ctx context.Context, phaseID int64, outcome, detail string, at time.Time) {
	if o.store == nil || phaseID == 0 {
		return
	}
	if err := o.store.FinishPhase(ctx, phaseID, outcome, detail, at); err != nil {
		o.storageError("finish lifecycle phase", err)
	}
}

func (o *Orchestrator) startStoredPhase(ctx context.Context, node Node, kind storage.PhaseKind, jobID int64, at time.Time) int64 {
	if o.store == nil || node.ResourceID == 0 {
		return 0
	}
	phase, err := o.store.StartPhase(ctx, storage.Phase{
		ResourceID: node.ResourceID, GenerationID: node.GenerationID, JobID: jobID,
		Kind: kind, StartedAt: at,
	})
	if err != nil {
		o.storageError("start "+string(kind)+" phase", err)
		return 0
	}
	return phase.ID
}

func (o *Orchestrator) beginResetGeneration(ctx context.Context, node Node, imageID string) (storage.Generation, error) {
	if o.store == nil || node.ResourceID == 0 {
		return storage.Generation{}, nil
	}
	if err := o.store.SetResourceState(ctx, node.ResourceID, storage.ResourceResetting, o.now()); err != nil {
		o.storageError("mark resource resetting", err)
		return storage.Generation{}, err
	}
	if node.GenerationID != 0 {
		if err := o.store.SetGenerationState(ctx, node.GenerationID, storage.GenerationClosed, o.now()); err != nil {
			o.storageError("close generation for reset", err)
			return storage.Generation{}, err
		}
	}
	generation, err := o.store.BeginGeneration(ctx, storage.Generation{
		ResourceID:  node.ResourceID,
		OperationID: fmt.Sprintf("reset-%s-%s", node.InstanceID, shortID()),
		ImageID:     imageID, Fingerprint: o.imageFingerprint(),
		State: storage.GenerationPreparing, StartedAt: o.now(),
	})
	o.storageError("begin reset generation", err)
	return generation, err
}
