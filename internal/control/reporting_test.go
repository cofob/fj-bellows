package control_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/hstern/fj-bellows/gen/fjbellows/control/v1"
	"github.com/hstern/fj-bellows/internal/control"
	mockctl "github.com/hstern/fj-bellows/internal/control/mock"
	"github.com/hstern/fj-bellows/internal/storage"
)

const (
	reportingRepository = "example/project"
	reportingWorkflow   = "ci.yml"
	reportingCurrency   = "USD"
	reportingRoute      = "amd64"
)

type reportingBackend struct {
	*mockctl.Backend
	historyFn    func(context.Context, storage.HistoryFilter) (storage.JobPage, error)
	statisticsFn func(context.Context, storage.StatisticsFilter) (storage.Statistics, error)
}

func (b *reportingBackend) JobHistory(
	ctx context.Context,
	filter storage.HistoryFilter,
) (storage.JobPage, error) {
	return b.historyFn(ctx, filter)
}

func (b *reportingBackend) Statistics(
	ctx context.Context,
	filter storage.StatisticsFilter,
) (storage.Statistics, error) {
	return b.statisticsFn(ctx, filter)
}

func TestJobHistoryRPCForwardsFiltersAndReportsIdentity(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	wantFilter := storage.HistoryFilter{
		From: from, To: to, Tier: selectedTier, Provider: selectedProvider,
		Repository: reportingRepository, Workflow: reportingWorkflow,
		Status: storage.JobSucceeded, Limit: 25, Cursor: "next-page",
	}
	backend := &reportingBackend{Backend: &mockctl.Backend{}}
	backend.historyFn = func(_ context.Context, filter storage.HistoryFilter) (storage.JobPage, error) {
		if !reflect.DeepEqual(filter, wantFilter) {
			return storage.JobPage{}, fmt.Errorf("history filter = %#v, want %#v", filter, wantFilter)
		}
		return storage.JobPage{
			Jobs: []storage.Job{{
				ID: 7, Source: "forgejo", Handle: "job-7", ForgejoJobID: 70, Attempt: 2,
				RepositoryID: 9, Repository: reportingRepository, Workflow: reportingWorkflow,
				WorkflowFile: ".forgejo/workflows/ci.yml", JobName: "test",
				IdentityQuality: storage.IdentityExact, Tier: selectedTier,
				Provider: selectedProvider, Driver: selectedDriver, Status: storage.JobSucceeded,
				Conclusion: "success", FirstSeenAt: from.Add(time.Minute), CompletedAt: from.Add(5 * time.Minute),
			}},
			NextCursor: "page-2",
		}, nil
	}

	_, client := newTestServer(t, backend)
	resp, err := client.JobHistory(t.Context(), connect.NewRequest(&controlv1.JobHistoryRequest{
		From: timestamppb.New(from), To: timestamppb.New(to),
		Tier: selectedTier, Provider: selectedProvider, Repository: reportingRepository,
		Workflow: reportingWorkflow, Status: string(storage.JobSucceeded), Limit: 25, Cursor: "next-page",
	}))
	if err != nil {
		t.Fatalf("JobHistory: %v", err)
	}
	if resp.Msg.NextCursor != "page-2" || len(resp.Msg.Jobs) != 1 {
		t.Fatalf("page = %+v", resp.Msg)
	}
	job := resp.Msg.Jobs[0]
	if job.Handle != "job-7" || job.Repository != reportingRepository || job.Workflow != reportingWorkflow ||
		job.Tier != selectedTier || job.Provider != selectedProvider || job.Driver != selectedDriver ||
		job.Status != string(storage.JobSucceeded) || job.IdentityQuality != string(storage.IdentityExact) {
		t.Fatalf("job identity/outcome = %+v", job)
	}
	if job.CompletedAt == nil || !job.CompletedAt.AsTime().Equal(from.Add(5*time.Minute)) {
		t.Fatalf("completed_at = %v", job.CompletedAt)
	}
}

//nolint:gocyclo // One RPC scenario verifies filter forwarding and all three reporting sections.
func TestStatisticsRPCForwardsFiltersAndReportsCostsAndTimings(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	wantFilter := storage.StatisticsFilter{
		From: from, To: to, Tier: selectedTier, Provider: selectedProvider,
		Repository: reportingRepository, Workflow: reportingWorkflow, Route: reportingRoute,
		GroupBy: storage.GroupWorkflow,
	}
	durations := storage.DurationSummary{
		Count: 2, Total: 9 * time.Minute, Min: 4 * time.Minute,
		Max: 5 * time.Minute, P50: 4 * time.Minute, P95: 5 * time.Minute,
	}
	backend := &reportingBackend{Backend: &mockctl.Backend{}}
	backend.statisticsFn = func(_ context.Context, filter storage.StatisticsFilter) (storage.Statistics, error) {
		if !reflect.DeepEqual(filter, wantFilter) {
			return storage.Statistics{}, fmt.Errorf("statistics filter = %#v, want %#v", filter, wantFilter)
		}
		return storage.Statistics{
			Groups: []storage.StatisticsGroup{{
				Key: storage.StatisticsKey{
					Source: "forgejo", RepositoryID: 9, Repository: reportingRepository,
					Workflow: reportingWorkflow, Tier: selectedTier, Provider: selectedProvider, Day: "2026-07-01",
				},
				Jobs: 2, Completed: 2, Succeeded: 1, Failed: 1, PricedJobs: 1, UnpricedJobs: 1,
				QueueDuration: durations, DispatchDuration: durations, RunDuration: durations,
				DirectCosts: []storage.CostTotal{{
					Kind: storage.CostDirectCompute, Currency: reportingCurrency,
					Nanos: 123, Entries: 1, UnknownEntries: 1, Estimated: true,
				}},
			}},
			FleetCosts: []storage.FleetCostTotal{{
				Tier: selectedTier, Provider: selectedProvider, Day: "2026-07-01",
				Kind: storage.CostWarmIdle, Currency: reportingCurrency, Nanos: 456, Entries: 2,
			}},
			FleetTimings: []storage.FleetTimingTotal{{
				Tier: selectedTier, Provider: selectedProvider, Day: "2026-07-01",
				Kind: storage.PhaseProvisioning, Duration: durations,
			}},
			Routing: []storage.RoutingEffectiveness{{
				Route: reportingRoute, RequiredLabel: "ci-auto-amd64", Currency: reportingCurrency,
				Decisions: 2, Completed: 1, FallbackDecisions: 1, HistoryDecisions: 1,
				IdleDecisions: 1, DeferredDecisions: 1, P95Hits: 1,
				EstimatedSelectedNanos: 100, EstimatedFallbackNanos: 150,
				EstimatedSavingsNanos: 50, ActualDirectNanos: 90, ActualUnknownEntries: 1,
				Selections: []storage.RoutingSelection{{Tier: selectedTier, Provider: selectedProvider, Jobs: 2}},
			}},
		}, nil
	}

	_, client := newTestServer(t, backend)
	resp, err := client.Statistics(t.Context(), connect.NewRequest(&controlv1.StatisticsRequest{
		From: timestamppb.New(from), To: timestamppb.New(to),
		Tier: selectedTier, Provider: selectedProvider, Repository: reportingRepository,
		Workflow: reportingWorkflow, GroupBy: string(storage.GroupWorkflow), Route: reportingRoute,
	}))
	if err != nil {
		t.Fatalf("Statistics: %v", err)
	}
	if len(resp.Msg.Groups) != 1 || len(resp.Msg.FleetCosts) != 1 || len(resp.Msg.FleetTimings) != 1 ||
		len(resp.Msg.RoutingEffectiveness) != 1 {
		t.Fatalf("statistics shape = %+v", resp.Msg)
	}
	group := resp.Msg.Groups[0]
	if group.Key.Workflow != reportingWorkflow || group.Key.Tier != selectedTier ||
		group.Jobs != 2 || group.Succeeded != 1 || group.Failed != 1 ||
		group.QueueDuration.Total.AsDuration() != durations.Total {
		t.Fatalf("statistics group = %+v", group)
	}
	if cost := group.DirectCosts[0]; cost.Kind != string(storage.CostDirectCompute) ||
		cost.Currency != reportingCurrency || cost.Nanos != 123 || cost.UnknownEntries != 1 || !cost.Estimated {
		t.Fatalf("direct cost = %+v", cost)
	}
	if fleetCost := resp.Msg.FleetCosts[0]; fleetCost.Kind != string(storage.CostWarmIdle) || fleetCost.Nanos != 456 {
		t.Fatalf("fleet cost = %+v", fleetCost)
	}
	if timing := resp.Msg.FleetTimings[0]; timing.Kind != string(storage.PhaseProvisioning) ||
		timing.Duration.P95.AsDuration() != durations.P95 {
		t.Fatalf("fleet timing = %+v", timing)
	}
	routing := resp.Msg.RoutingEffectiveness[0]
	if routing.Route != reportingRoute || routing.Decisions != 2 || routing.P95Hits != 1 ||
		routing.EstimatedSavingsNanos != 50 || len(routing.Selections) != 1 ||
		routing.Selections[0].Tier != selectedTier {
		t.Fatalf("routing effectiveness = %+v", routing)
	}
}

var _ control.Backend = (*reportingBackend)(nil)
