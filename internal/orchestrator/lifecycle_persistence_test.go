package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hstern/fj-bellows/internal/forgejo"
	omock "github.com/hstern/fj-bellows/internal/orchestrator/mock"
	"github.com/hstern/fj-bellows/internal/provider"
	pmock "github.com/hstern/fj-bellows/internal/provider/mock"
	"github.com/hstern/fj-bellows/internal/storage"
	smock "github.com/hstern/fj-bellows/internal/storage/mock"
)

func TestSyncPoolDrainsDurableResourceWithoutGeneration(t *testing.T) {
	const (
		instanceID = "vm-missing-generation"
		resourceID = int64(41)
	)
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	resource := storage.Resource{
		ID: resourceID, Provider: fleetHetznerName, Driver: fleetHetznerDriver, Tier: fleetLongTier,
		ExternalID: instanceID, Name: "fjb-long-recovered", State: storage.ResourceActive,
		ProviderCreatedAt: now.Add(-20 * time.Minute),
	}
	store := &smock.Store{
		OpenResourcesFn: func(context.Context) ([]storage.Resource, error) {
			return []storage.Resource{resource}, nil
		},
		OpenGenerationsFn: func(context.Context) ([]storage.Generation, error) {
			return nil, nil
		},
		StartPhaseFn: func(_ context.Context, phase storage.Phase) (storage.Phase, error) {
			if phase.Kind != storage.PhaseRemoving {
				t.Errorf("adoption phase kind = %q, want %q", phase.Kind, storage.PhaseRemoving)
			}
			phase.ID = 77
			return phase, nil
		},
	}
	jobs := &omock.JobSource{}
	dispatcher := &omock.Dispatcher{}
	cfg := baseConfig()
	cfg.Tier = fleetLongTier
	cfg.ProviderName = fleetHetznerName
	cfg.Driver = fleetHetznerDriver
	o := New(cfg, &pmock.Provider{}, jobs, dispatcher, nil)
	o.SetStore(store)
	o.now = func() time.Time { return now }

	adopted, dropped, err := o.syncPool(t.Context(), []provider.Instance{{
		ID: instanceID, Name: resource.Name, IPv4: testIP, CreatedAt: resource.ProviderCreatedAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if adopted != 1 || dropped != 0 {
		t.Fatalf("syncPool() = adopted %d, dropped %d; want 1, 0", adopted, dropped)
	}
	node, ok := o.pool.Get(instanceID)
	if !ok {
		t.Fatal("recovered resource was not adopted")
	}
	if node.State != StateDraining {
		t.Fatalf("adopted state = %q, want %q", node.State, StateDraining)
	}
	if node.ResourceID != resourceID || node.GenerationID != 0 {
		t.Fatalf("adopted durable IDs = resource %d, generation %d; want %d, 0",
			node.ResourceID, node.GenerationID, resourceID)
	}

	dispatched, provisioned := o.dispatchJobs(t.Context(), []forgejo.WaitingJob{{
		Handle: "job-must-not-see-dirty-disk", Labels: []string{labelUbuntu},
	}})
	if dispatched != 0 || provisioned != 0 {
		t.Fatalf("dispatchJobs() = dispatched %d, provisioned %d; want 0, 0", dispatched, provisioned)
	}
	if jobs.RegisterCount() != 0 || dispatcher.RunCount() != 0 {
		t.Fatalf("dirty resource reached dispatch: registrations=%d runs=%d",
			jobs.RegisterCount(), dispatcher.RunCount())
	}
}

//nolint:gocyclo // One scenario asserts every ordered durability boundary of shutdown teardown.
func TestDestroyAllPersistsMutationPhaseAndResourceClosure(t *testing.T) {
	const (
		instanceID    = "vm-shutdown"
		resourceID    = int64(51)
		generationID  = int64(52)
		readyPhaseID  = int64(53)
		removePhaseID = int64(54)
	)
	now := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	var (
		begunMutation       storage.Mutation
		finishedMutationID  string
		finishedMutation    storage.MutationState
		finishedExternalID  string
		resourceState       storage.ResourceState
		closedResourceState storage.ResourceState
		closedReason        string
		generationState     storage.GenerationState
		finishedPhases      = make(map[int64]string)
	)
	store := &smock.Store{
		BeginMutationFn: func(_ context.Context, mutation storage.Mutation) error {
			begunMutation = mutation
			return nil
		},
		SetResourceStateFn: func(_ context.Context, id int64, state storage.ResourceState, _ time.Time) error {
			if id != resourceID {
				t.Errorf("SetResourceState resource = %d, want %d", id, resourceID)
			}
			resourceState = state
			return nil
		},
		StartPhaseFn: func(_ context.Context, phase storage.Phase) (storage.Phase, error) {
			if phase.ResourceID != resourceID || phase.GenerationID != generationID || phase.Kind != storage.PhaseRemoving {
				t.Errorf("removal phase = %+v", phase)
			}
			phase.ID = removePhaseID
			return phase, nil
		},
		FinishPhaseFn: func(_ context.Context, id int64, outcome, _ string, _ time.Time) error {
			finishedPhases[id] = outcome
			return nil
		},
		FinishMutationFn: func(_ context.Context, operationID string, state storage.MutationState, externalID, _ string, _ time.Time) error {
			finishedMutationID = operationID
			finishedMutation = state
			finishedExternalID = externalID
			return nil
		},
		SetGenerationStateFn: func(_ context.Context, id int64, state storage.GenerationState, _ time.Time) error {
			if id != generationID {
				t.Errorf("SetGenerationState generation = %d, want %d", id, generationID)
			}
			generationState = state
			return nil
		},
		CloseResourceFn: func(_ context.Context, id int64, state storage.ResourceState, reason string, _ time.Time) error {
			if id != resourceID {
				t.Errorf("CloseResource resource = %d, want %d", id, resourceID)
			}
			closedResourceState = state
			closedReason = reason
			return nil
		},
	}
	providerMock := &pmock.Provider{}
	o := New(baseConfig(), providerMock, &omock.JobSource{}, &omock.Dispatcher{}, nil)
	o.SetStore(store)
	o.now = func() time.Time { return now }
	o.pool.Put(&Node{
		InstanceID: instanceID, ResourceID: resourceID, GenerationID: generationID,
		PhaseID: readyPhaseID, State: StateBusy,
	})

	o.destroyAll()

	if providerMock.DestroyCount() != 1 {
		t.Fatalf("DestroyCount = %d, want 1", providerMock.DestroyCount())
	}
	if begunMutation.Kind != "destroy_on_exit" || begunMutation.ResourceID != resourceID ||
		begunMutation.State != storage.MutationPending {
		t.Fatalf("begin mutation = %+v", begunMutation)
	}
	if resourceState != storage.ResourceDestroying {
		t.Errorf("resource pre-destroy state = %q, want %q", resourceState, storage.ResourceDestroying)
	}
	if finishedPhases[readyPhaseID] != "shutdown" || finishedPhases[removePhaseID] != "destroyed" {
		t.Errorf("finished phases = %+v; want ready=shutdown and removing=destroyed", finishedPhases)
	}
	if finishedMutationID != begunMutation.OperationID || finishedMutation != storage.MutationSucceeded ||
		finishedExternalID != instanceID {
		t.Errorf("finished mutation = id %q, state %q, external %q; begin id %q",
			finishedMutationID, finishedMutation, finishedExternalID, begunMutation.OperationID)
	}
	if generationState != storage.GenerationClosed {
		t.Errorf("generation state = %q, want %q", generationState, storage.GenerationClosed)
	}
	if closedResourceState != storage.ResourceClosed || closedReason != "shutdown" {
		t.Errorf("resource closure = state %q, reason %q; want closed, shutdown",
			closedResourceState, closedReason)
	}
	if _, ok := o.pool.Get(instanceID); ok {
		t.Error("destroyed worker remains in pool")
	}
}

func TestApplyTeardownRecordsFinalWarmIdleInterval(t *testing.T) {
	const (
		instanceID = "vm-idle"
		resourceID = int64(61)
		quoteID    = int64(62)
	)
	now := time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC)
	lastBusy := now.Add(-12 * time.Minute)
	var (
		costMu sync.Mutex
		costs  []storage.CostEntry
	)
	store := &smock.Store{
		StartPhaseFn: func(_ context.Context, phase storage.Phase) (storage.Phase, error) {
			phase.ID = 64
			return phase, nil
		},
		GetPriceQuoteFn: func(_ context.Context, id int64) (storage.PriceQuote, error) {
			if id != quoteID {
				t.Errorf("GetPriceQuote ID = %d, want %d", id, quoteID)
			}
			return storage.PriceQuote{ID: quoteID, Currency: testCostCurrency, PerHourNanos: 3600}, nil
		},
		RecordCostFn: func(_ context.Context, entry storage.CostEntry) (storage.CostEntry, error) {
			costMu.Lock()
			costs = append(costs, entry)
			costMu.Unlock()
			return entry, nil
		},
	}
	providerMock := &pmock.Provider{}
	cfg := baseConfig()
	cfg.Teardown.IdleTimeout = 5 * time.Minute
	o := New(cfg, providerMock, &omock.JobSource{}, &omock.Dispatcher{}, nil)
	o.SetStore(store)
	o.now = func() time.Time { return now }
	o.pool.Put(&Node{
		InstanceID: instanceID, ResourceID: resourceID, PriceQuoteID: quoteID,
		PhaseID: 63, State: StateIdle, CreatedAt: now.Add(-30 * time.Minute), LastBusy: lastBusy,
	})

	if got := o.applyTeardown(t.Context()); got != 1 {
		t.Fatalf("applyTeardown() = %d, want 1", got)
	}
	o.wg.Wait()
	if providerMock.DestroyCount() != 1 {
		t.Fatalf("DestroyCount = %d, want 1", providerMock.DestroyCount())
	}

	costMu.Lock()
	defer costMu.Unlock()
	var warm []storage.CostEntry
	for _, entry := range costs {
		if entry.Kind == storage.CostWarmIdle {
			warm = append(warm, entry)
		}
	}
	if len(warm) != 1 {
		t.Fatalf("warm-idle entries = %d, want 1; all costs = %+v", len(warm), costs)
	}
	entry := warm[0]
	if entry.ResourceID != resourceID || entry.PriceQuoteID != quoteID ||
		!entry.StartedAt.Equal(lastBusy) || !entry.EndedAt.Equal(now) {
		t.Errorf("warm-idle interval = %+v", entry)
	}
	if !entry.Known || entry.Currency != testCostCurrency || entry.Nanos != 720 {
		t.Errorf("warm-idle estimate = known %v, currency %q, nanos %d; want true, EUR, 720",
			entry.Known, entry.Currency, entry.Nanos)
	}
}

func TestProvisionGenerationPersistenceFailureNeverPublishesIdle(t *testing.T) {
	const (
		instanceID   = "vm-ready-storage-failure"
		resourceID   = int64(71)
		generationID = int64(72)
	)
	readyErr := errors.New("database unavailable while publishing generation")
	var (
		o            *Orchestrator
		stateAtWrite NodeState
		stateSeen    bool
	)
	store := &smock.Store{
		BeginResourceFn: func(_ context.Context, resource storage.Resource) (storage.Resource, error) {
			resource.ID = resourceID
			return resource, nil
		},
		StartPhaseFn: func(_ context.Context, phase storage.Phase) (storage.Phase, error) {
			phase.ID = 73
			return phase, nil
		},
		BeginGenerationFn: func(_ context.Context, generation storage.Generation) (storage.Generation, error) {
			generation.ID = generationID
			return generation, nil
		},
		SetGenerationStateFn: func(_ context.Context, id int64, state storage.GenerationState, _ time.Time) error {
			if id != generationID {
				t.Errorf("SetGenerationState generation = %d, want %d", id, generationID)
			}
			if state != storage.GenerationReady {
				return nil
			}
			node, ok := o.pool.Get(instanceID)
			if !ok {
				t.Error("worker missing while its ready generation was being persisted")
			} else {
				stateAtWrite = node.State
				stateSeen = true
			}
			return readyErr
		},
	}
	providerMock := &pmock.Provider{
		ProvisionFn: func(context.Context, provider.Spec) (provider.Instance, error) {
			return provider.Instance{
				ID: instanceID, IPv4: testIP,
				CreatedAt: time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	dispatcher := &omock.Dispatcher{}
	o = New(baseConfig(), providerMock, &omock.JobSource{}, dispatcher, nil)
	o.SetStore(store)

	o.provisionOne(t.Context())
	o.wg.Wait()

	if dispatcher.RunCount() != 0 {
		t.Fatalf("RunCount = %d, want 0", dispatcher.RunCount())
	}
	if !stateSeen {
		t.Fatal("ready generation persistence was not attempted")
	}
	if stateAtWrite != StateProvisioning {
		t.Fatalf("worker state during ready-generation write = %q, want %q", stateAtWrite, StateProvisioning)
	}
	node, ok := o.pool.Get(instanceID)
	if !ok {
		t.Fatal("worker unexpectedly disappeared after generation persistence failure")
	}
	if node.State != StateDraining {
		t.Fatalf("worker state after generation persistence failure = %q, want %q", node.State, StateDraining)
	}
	if got := len(o.pool.ByState(StateIdle)); got != 0 {
		t.Fatalf("idle workers = %d, want 0", got)
	}

	dispatched, provisioned := o.dispatchJobs(t.Context(), []forgejo.WaitingJob{{
		Handle: "job-after-ready-write-failure", Labels: []string{labelUbuntu},
	}})
	if dispatched != 0 || provisioned != 0 {
		t.Fatalf("dispatchJobs() = dispatched %d, provisioned %d; want 0, 0", dispatched, provisioned)
	}
}

//nolint:gocyclo // The table verifies both provider-delete outcomes and all safety assertions together.
func TestPostJobGenerationPersistenceFailureNeverReusesWorker(t *testing.T) {
	tests := []struct {
		name           string
		destroyErr     error
		wantPresent    bool
		wantFinalState NodeState
	}{
		{name: "destroy succeeds"},
		{name: "destroy fails and drains", destroyErr: errors.New("provider delete failed"), wantPresent: true, wantFinalState: StateDraining},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				instanceID   = "vm-job-storage-failure"
				resourceID   = int64(81)
				generationID = int64(82)
			)
			generationErr := errors.New("database unavailable after job")
			var (
				o               *Orchestrator
				stateMu         sync.Mutex
				stateAtReady    NodeState
				readyWriteSeen  bool
				closedReason    string
				phaseSequenceID int64 = 90
			)
			store := &smock.Store{
				UpsertJobFn: func(_ context.Context, job storage.Job) (storage.Job, error) {
					job.ID = 83
					return job, nil
				},
				SetGenerationStateFn: func(_ context.Context, id int64, state storage.GenerationState, _ time.Time) error {
					if id != generationID {
						t.Errorf("SetGenerationState generation = %d, want %d", id, generationID)
					}
					if state != storage.GenerationReady {
						return nil
					}
					node, ok := o.pool.Get(instanceID)
					stateMu.Lock()
					readyWriteSeen = true
					if ok {
						stateAtReady = node.State
					}
					stateMu.Unlock()
					if !ok {
						t.Error("worker missing while its post-job generation was being persisted")
					}
					return generationErr
				},
				StartPhaseFn: func(_ context.Context, phase storage.Phase) (storage.Phase, error) {
					phaseSequenceID++
					phase.ID = phaseSequenceID
					return phase, nil
				},
				CloseResourceFn: func(_ context.Context, id int64, _ storage.ResourceState, reason string, _ time.Time) error {
					if id != resourceID {
						t.Errorf("CloseResource resource = %d, want %d", id, resourceID)
					}
					closedReason = reason
					return nil
				},
			}
			providerMock := &pmock.Provider{
				DestroyFn: func(context.Context, string) error { return tt.destroyErr },
			}
			o = New(baseConfig(), providerMock, &omock.JobSource{}, &omock.Dispatcher{}, nil)
			o.SetStore(store)
			o.pool.Put(&Node{
				InstanceID: instanceID, ResourceID: resourceID, GenerationID: generationID,
				PhaseID: 84, State: StateIdle, IP: testIP,
				CreatedAt: time.Now().Add(-20 * time.Minute), LastBusy: time.Now().Add(-time.Minute),
			})

			if !o.dispatch(t.Context(), Node{
				InstanceID: instanceID, ResourceID: resourceID, GenerationID: generationID,
				PhaseID: 84, State: StateIdle, IP: testIP,
			}, forgejo.WaitingJob{Handle: "completed-job", Labels: []string{labelUbuntu}}) {
				t.Fatal("dispatch() = false, want true")
			}
			o.wg.Wait()

			stateMu.Lock()
			seen, observedState := readyWriteSeen, stateAtReady
			stateMu.Unlock()
			if !seen {
				t.Fatal("post-job ready generation persistence was not attempted")
			}
			if observedState != StateBusy {
				t.Fatalf("worker state during post-job generation write = %q, want %q", observedState, StateBusy)
			}
			if providerMock.DestroyCount() != 1 {
				t.Fatalf("DestroyCount = %d, want 1", providerMock.DestroyCount())
			}
			node, present := o.pool.Get(instanceID)
			if present != tt.wantPresent {
				t.Fatalf("worker present = %v, want %v (node=%+v)", present, tt.wantPresent, node)
			}
			if present && node.State != tt.wantFinalState {
				t.Fatalf("worker final state = %q, want %q", node.State, tt.wantFinalState)
			}
			if len(o.pool.ByState(StateIdle)) != 0 {
				t.Fatal("post-job persistence failure returned worker to the idle pool")
			}
			if tt.destroyErr == nil && closedReason != "generation_persistence_failed" {
				t.Errorf("resource close reason = %q, want generation_persistence_failed", closedReason)
			}
		})
	}
}
