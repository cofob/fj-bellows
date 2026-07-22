package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/hstern/fj-bellows/gen/fjbellows/control/v1"
	"github.com/hstern/fj-bellows/gen/fjbellows/control/v1/controlv1connect"
	"github.com/hstern/fj-bellows/internal/control/logbus"
	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/storage"
)

// defaultLogHistoryLines is the replay-on-connect size when a StreamLogs
// client doesn't ask for a specific count. A few hundred lines is enough
// for "what just happened?" without dumping the whole ring buffer.
const defaultLogHistoryLines = 100

// apiHandler adapts a Backend to the generated ConnectRPC service surface.
// Keeping protobuf imports in this file (and not in the orchestrator package)
// means the orchestrator stays free of generated-code coupling.
type apiHandler struct {
	controlv1connect.UnimplementedControlServiceHandler
	b Backend
	// enableWrites gates the mutating ForceReap / ForceProvision RPCs.
	// When false, those RPCs short-circuit to CodePermissionDenied before
	// touching the backend. Read-only RPCs ignore it entirely.
	enableWrites bool
}

// errWritesDisabled is the response body for force-* RPCs when the
// -enable-control-writes flag is unset. Operator-facing: tells them which
// flag to flip.
var errWritesDisabled = errors.New("control writes not enabled (set -enable-control-writes)")

func (h *apiHandler) Health(
	ctx context.Context,
	_ *connect.Request[controlv1.HealthRequest],
) (*connect.Response[controlv1.HealthResponse], error) {
	s := h.b.Health(ctx)
	return connect.NewResponse(&controlv1.HealthResponse{
		Healthy:                       s.Healthy,
		LastTickAt:                    tsOrNil(s.LastTickAt),
		LastProviderListAt:            tsOrNil(s.LastProviderListAt),
		LastForgejoPollAt:             tsOrNil(s.LastForgejoPollAt),
		Paused:                        s.Paused,
		DatabaseHealthy:               s.DatabaseHealthy,
		DatabaseLastSuccessfulWriteAt: tsOrNil(s.DatabaseLastSuccessfulWrite),
		DatabaseLastError:             s.DatabaseLastError,
		RoutingHealthy:                s.RoutingHealthy,
		RoutingLastPollAt:             tsOrNil(s.RoutingLastPollAt),
		RoutingLastDecisionAt:         tsOrNil(s.RoutingLastDecisionAt),
		RoutingLastError:              s.RoutingLastError,
		RoutingDegradedPricing:        s.RoutingDegradedPricing,
	}), nil
}

func (h *apiHandler) ListWorkers(
	_ context.Context,
	req *connect.Request[controlv1.ListWorkersRequest],
) (*connect.Response[controlv1.ListWorkersResponse], error) {
	view := h.b.PoolSnapshot()
	if selector, ok := h.b.(interface {
		PoolSnapshotFiltered(tier, provider string) []WorkerView
	}); ok {
		view = selector.PoolSnapshotFiltered(req.Msg.Tier, req.Msg.Provider)
	} else {
		view = filterWorkerViews(view, req.Msg.Tier, req.Msg.Provider)
	}
	workers := make([]*controlv1.Worker, 0, len(view))
	for _, w := range view {
		workers = append(workers, &controlv1.Worker{
			InstanceId:     w.InstanceID,
			State:          w.State,
			Ip:             w.IP,
			VpcIp:          w.VPCIP,
			CreatedAt:      tsOrNil(w.CreatedAt),
			LastBusy:       tsOrNil(w.LastBusy),
			CurrentJob:     w.CurrentJob,
			PaidHourEndAt:  tsOrNil(w.PaidHourEndAt),
			ReapEligibleAt: tsOrNil(w.ReapEligibleAt),
			BillingModel:   w.BillingModel,
			Tier:           w.Tier,
			Provider:       w.ProviderName,
			Driver:         w.Driver,
		})
	}
	return connect.NewResponse(&controlv1.ListWorkersResponse{Workers: workers}), nil
}

func (h *apiHandler) GetCache(
	ctx context.Context,
	req *connect.Request[controlv1.GetCacheRequest],
) (*connect.Response[controlv1.GetCacheResponse], error) {
	s := h.b.CacheStatus(ctx)
	if selector, ok := h.b.(interface {
		CacheStatusFor(context.Context, string) *CacheStatus
	}); ok {
		s = selector.CacheStatusFor(ctx, req.Msg.Provider)
	}
	if s == nil {
		return connect.NewResponse(&controlv1.GetCacheResponse{Present: false}), nil
	}
	return connect.NewResponse(&controlv1.GetCacheResponse{
		Present:         s.Present,
		AdoptedExisting: s.AdoptedExisting,
		LinodeId:        int64(s.LinodeID),
		VpcIp:           s.VPCIP,
		BucketRegion:    s.BucketRegion,
		BucketLabel:     s.BucketLabel,
		VmState:         s.VMState,
	}), nil
}

func (h *apiHandler) Reconcile(
	ctx context.Context,
	_ *connect.Request[controlv1.ReconcileRequest],
) (*connect.Response[controlv1.ReconcileResponse], error) {
	r, err := h.b.Kick(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&controlv1.ReconcileResponse{
		//nolint:gosec // counts come from in-process int counters; can't overflow int32 in practice
		Provisioned: int32(r.Provisioned),
		//nolint:gosec // see above
		Dispatched: int32(r.Dispatched),
		//nolint:gosec // see above
		Reaped: int32(r.Reaped),
		//nolint:gosec // see above
		Adopted: int32(r.Adopted),
		//nolint:gosec // see above
		Dropped: int32(r.Dropped),
		Errors:  r.Errors,
	}), nil
}

func (h *apiHandler) StreamEvents(
	ctx context.Context,
	req *connect.Request[controlv1.StreamEventsRequest],
	stream *connect.ServerStream[controlv1.StreamEventsResponse],
) error {
	ch, cancel := h.b.Subscribe()
	defer cancel()
	// Send a sentinel event immediately so the client's call returns
	// without waiting for the first real event. Connect server-streaming
	// only writes response headers on the first Send; without this, a
	// quiet daemon would make the client appear to hang on Open.
	if err := stream.Send(&controlv1.StreamEventsResponse{
		At:   tsOrNil(time.Now()),
		Type: "stream_opened",
	}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-ch:
			if !ok {
				// Bus dropped us for slow consumption.
				return connect.NewError(connect.CodeResourceExhausted,
					errors.New("stream subscriber dropped: client too slow"))
			}
			if req.Msg.Tier != "" && ev.Attrs["tier"] != req.Msg.Tier {
				continue
			}
			if req.Msg.Provider != "" && ev.Attrs["provider"] != req.Msg.Provider {
				continue
			}
			if err := stream.Send(&controlv1.StreamEventsResponse{
				At:    tsOrNil(ev.At),
				Type:  ev.Type,
				Attrs: ev.Attrs,
			}); err != nil {
				return err
			}
		}
	}
}

func (h *apiHandler) StreamLogs(
	ctx context.Context,
	req *connect.Request[controlv1.StreamLogsRequest],
	stream *connect.ServerStream[controlv1.StreamLogsResponse],
) error {
	filter := logbus.Filter{
		InstanceID: req.Msg.InstanceId,
		Handle:     req.Msg.Handle,
	}
	// Pick replay size: explicit request wins (capped at ring capacity);
	// otherwise replay defaultLogHistoryLines.
	history := int(req.Msg.HistoryLines)
	history = max(history, 0)
	if req.Msg.HistoryLines == 0 {
		history = defaultLogHistoryLines
	}
	history = min(history, logbus.HistoryCapacity)

	// Subscribe BEFORE fetching history so any record published between
	// the history snapshot and the first Recv lands in the subscriber
	// buffer rather than being dropped.
	ch, cancel := h.b.SubscribeLogs(filter)
	defer cancel()

	// Sentinel: makes the open call return immediately on a quiet daemon
	// (Connect server-streaming only writes response headers on first
	// Send). Same convention as StreamEvents.
	if err := stream.Send(&controlv1.StreamLogsResponse{
		At: tsOrNil(time.Now()),
	}); err != nil {
		return err
	}

	if history > 0 {
		for _, r := range h.b.LogHistory(history, filter) {
			if err := stream.Send(logRecordToResponse(r)); err != nil {
				return err
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case r, ok := <-ch:
			if !ok {
				return connect.NewError(connect.CodeResourceExhausted,
					errors.New("stream subscriber dropped: client too slow"))
			}
			if err := stream.Send(logRecordToResponse(r)); err != nil {
				return err
			}
		}
	}
}

func logRecordToResponse(r logbus.Record) *controlv1.StreamLogsResponse {
	return &controlv1.StreamLogsResponse{
		At:      tsOrNil(r.At),
		Level:   r.Level.String(),
		Message: r.Message,
		Attrs:   r.Attrs,
	}
}

func (h *apiHandler) ProviderInfo(
	ctx context.Context,
	req *connect.Request[controlv1.ProviderInfoRequest],
) (*connect.Response[controlv1.ProviderInfoResponse], error) {
	name, info := h.b.ProviderInfo(ctx)
	if selector, ok := h.b.(interface {
		ProviderInfoFor(context.Context, string) (string, map[string]string)
	}); ok {
		name, info = selector.ProviderInfoFor(ctx, req.Msg.Provider)
	}
	// Nil maps marshal to an empty proto map; defensively normalise so
	// the wire form is always present and clients don't have to nil-
	// check Info themselves.
	if info == nil {
		info = map[string]string{}
	}
	return connect.NewResponse(&controlv1.ProviderInfoResponse{
		Provider: name,
		Info:     info,
	}), nil
}

func (h *apiHandler) ForceReap(
	ctx context.Context,
	req *connect.Request[controlv1.ForceReapRequest],
) (*connect.Response[controlv1.ForceReapResponse], error) {
	if !h.enableWrites {
		return nil, connect.NewError(connect.CodePermissionDenied, errWritesDisabled)
	}
	id := req.Msg.InstanceId
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_id is required"))
	}
	ctx = orchestrator.WithAuditCaller(ctx, auditCaller(req))
	var err error
	if selector, ok := h.b.(interface {
		ForceReapIn(context.Context, string, string) error
	}); ok {
		err = selector.ForceReapIn(ctx, req.Msg.Tier, id)
	} else {
		err = h.b.ForceReap(ctx, id)
	}
	if err != nil {
		// "not in pool" is a 4xx (the operator named something that
		// doesn't exist); other failures are 5xx.
		if strings.Contains(err.Error(), "not in pool") || strings.Contains(err.Error(), "vanished from pool") {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&controlv1.ForceReapResponse{}), nil
}

func (h *apiHandler) GetConfig(
	ctx context.Context,
	_ *connect.Request[controlv1.GetConfigRequest],
) (*connect.Response[controlv1.GetConfigResponse], error) {
	yamlText, path := h.b.GetConfig(ctx)
	return connect.NewResponse(&controlv1.GetConfigResponse{
		Yaml:       yamlText,
		ConfigPath: path,
	}), nil
}

// ReloadConfig structurally mirrors ForceProvision (write-gate + audit-caller
// + backend call + error map) but the error code mapping differs
// (FailedPrecondition vs Internal); extracting a helper would have to take
// the error mapper as a func, which is more boilerplate than the duplication
// it removes.
func (h *apiHandler) ReloadConfig(
	ctx context.Context,
	req *connect.Request[controlv1.ReloadConfigRequest],
) (*connect.Response[controlv1.ReloadConfigResponse], error) {
	if !h.enableWrites {
		return nil, connect.NewError(connect.CodePermissionDenied, errWritesDisabled)
	}
	ctx = orchestrator.WithAuditCaller(ctx, auditCaller(req))
	changed, err := h.b.ReloadConfig(ctx)
	if err != nil {
		// "reload rejected" (non-hot field changed) is operator-facing
		// "your config can't be hot-swapped" — a precondition failure,
		// not an internal one. Read I/O / parse errors are also
		// FailedPrecondition because they're on-disk state the operator
		// is responsible for. We keep one error class for the whole
		// "can't reload" bucket so clients don't need to switch.
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewResponse(&controlv1.ReloadConfigResponse{
		ChangedFields: changed,
	}), nil
}

func (h *apiHandler) ForceProvision(
	ctx context.Context,
	req *connect.Request[controlv1.ForceProvisionRequest],
) (*connect.Response[controlv1.ForceProvisionResponse], error) {
	if !h.enableWrites {
		return nil, connect.NewError(connect.CodePermissionDenied, errWritesDisabled)
	}
	ctx = orchestrator.WithAuditCaller(ctx, auditCaller(req))
	var id string
	var err error
	if selector, ok := h.b.(interface {
		ForceProvisionIn(context.Context, string) (string, error)
	}); ok {
		id, err = selector.ForceProvisionIn(ctx, req.Msg.Tier)
	} else {
		id, err = h.b.ForceProvision(ctx)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&controlv1.ForceProvisionResponse{InstanceId: id}), nil
}

func (h *apiHandler) Pause(
	ctx context.Context,
	req *connect.Request[controlv1.PauseRequest],
) (*connect.Response[controlv1.PauseResponse], error) {
	if !h.enableWrites {
		return nil, connect.NewError(connect.CodePermissionDenied, errWritesDisabled)
	}
	ctx = orchestrator.WithAuditCaller(ctx, auditCaller(req))
	h.b.Pause(ctx)
	return connect.NewResponse(&controlv1.PauseResponse{}), nil
}

func (h *apiHandler) Resume(
	ctx context.Context,
	req *connect.Request[controlv1.ResumeRequest],
) (*connect.Response[controlv1.ResumeResponse], error) {
	if !h.enableWrites {
		return nil, connect.NewError(connect.CodePermissionDenied, errWritesDisabled)
	}
	ctx = orchestrator.WithAuditCaller(ctx, auditCaller(req))
	h.b.Resume(ctx)
	return connect.NewResponse(&controlv1.ResumeResponse{}), nil
}

// execCommandLimit mirrors orchestrator.execCommandLimit (64 KiB). Kept
// here as a local constant so the handler can reject early without
// reaching for the orchestrator package's internal limits.
const execCommandLimit = 64 * 1024

func (h *apiHandler) ExecOnWorker(
	ctx context.Context,
	req *connect.Request[controlv1.ExecOnWorkerRequest],
) (*connect.Response[controlv1.ExecOnWorkerResponse], error) {
	if !h.enableWrites {
		return nil, connect.NewError(connect.CodePermissionDenied, errWritesDisabled)
	}
	id := req.Msg.InstanceId
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_id is required"))
	}
	cmd := req.Msg.Command
	if cmd == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("command is required"))
	}
	if len(cmd) > execCommandLimit {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("command too long"))
	}
	ctx = orchestrator.WithAuditCaller(ctx, auditCaller(req))
	var stdout, stderr []byte
	var exitCode int32
	var truncStdout, truncStderr int64
	var err error
	if selector, ok := h.b.(interface {
		ExecOnWorkerIn(context.Context, string, string, string) ([]byte, []byte, int32, int64, int64, error)
	}); ok {
		stdout, stderr, exitCode, truncStdout, truncStderr, err = selector.ExecOnWorkerIn(ctx, req.Msg.Tier, id, cmd)
	} else {
		stdout, stderr, exitCode, truncStdout, truncStderr, err = h.b.ExecOnWorker(ctx, id, cmd)
	}
	if err != nil {
		// "not in pool" / "vanished from pool" → CodeNotFound.
		// "exec is not supported" (docker provider) → CodeUnimplemented.
		// "instance in state ... exec requires idle or busy" →
		// CodeFailedPrecondition (caller named a real but transitional node).
		// Everything else (SSH dial, transport, ...) → CodeInternal.
		if errors.Is(err, orchestrator.ErrExecNotSupported) {
			return nil, connect.NewError(connect.CodeUnimplemented, err)
		}
		msg := err.Error()
		if strings.Contains(msg, "not in pool") || strings.Contains(msg, "vanished from pool") {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		if strings.Contains(msg, "exec requires idle or busy") {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&controlv1.ExecOnWorkerResponse{
		Stdout:          stdout,
		Stderr:          stderr,
		ExitCode:        exitCode,
		TruncatedStdout: truncStdout,
		TruncatedStderr: truncStderr,
	}), nil
}

func (h *apiHandler) JobHistory(
	ctx context.Context,
	req *connect.Request[controlv1.JobHistoryRequest],
) (*connect.Response[controlv1.JobHistoryResponse], error) {
	reporter, ok := h.b.(interface {
		JobHistory(context.Context, storage.HistoryFilter) (storage.JobPage, error)
	})
	if !ok {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("durable job history is unavailable"))
	}
	from, err := validProtoTime(req.Msg.From)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	to, err := validProtoTime(req.Msg.To)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	page, err := reporter.JobHistory(ctx, storage.HistoryFilter{
		From: from, To: to, Tier: req.Msg.Tier, Provider: req.Msg.Provider,
		Repository: req.Msg.Repository, Workflow: req.Msg.Workflow,
		Status: storage.JobStatus(req.Msg.Status), Limit: int(req.Msg.Limit), Cursor: req.Msg.Cursor,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	jobs := make([]*controlv1.Job, 0, len(page.Jobs))
	for _, job := range page.Jobs {
		jobs = append(jobs, jobToProto(job))
	}
	return connect.NewResponse(&controlv1.JobHistoryResponse{Jobs: jobs, NextCursor: page.NextCursor}), nil
}

func (h *apiHandler) Statistics(
	ctx context.Context,
	req *connect.Request[controlv1.StatisticsRequest],
) (*connect.Response[controlv1.StatisticsResponse], error) {
	reporter, ok := h.b.(interface {
		Statistics(context.Context, storage.StatisticsFilter) (storage.Statistics, error)
	})
	if !ok {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("durable statistics are unavailable"))
	}
	from, err := validProtoTime(req.Msg.From)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	to, err := validProtoTime(req.Msg.To)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	stats, err := reporter.Statistics(ctx, storage.StatisticsFilter{
		From: from, To: to, Tier: req.Msg.Tier, Provider: req.Msg.Provider,
		Repository: req.Msg.Repository, Workflow: req.Msg.Workflow,
		Route:   req.Msg.Route,
		GroupBy: storage.StatisticsGroupBy(req.Msg.GroupBy),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	response := &controlv1.StatisticsResponse{
		Groups:       make([]*controlv1.StatisticsGroup, 0, len(stats.Groups)),
		FleetCosts:   make([]*controlv1.FleetCostTotal, 0, len(stats.FleetCosts)),
		FleetTimings: make([]*controlv1.FleetTimingTotal, 0, len(stats.FleetTimings)),
		RoutingEffectiveness: make(
			[]*controlv1.RoutingEffectiveness, 0, len(stats.Routing),
		),
	}
	for _, group := range stats.Groups {
		response.Groups = append(response.Groups, statisticsGroupToProto(group))
	}
	for _, total := range stats.FleetCosts {
		response.FleetCosts = append(response.FleetCosts, &controlv1.FleetCostTotal{
			Tier: total.Tier, Provider: total.Provider, Day: total.Day,
			Kind: string(total.Kind), Currency: total.Currency, Nanos: total.Nanos,
			Entries: total.Entries, UnknownEntries: total.UnknownEntries, Estimated: total.Estimated,
		})
	}
	for _, total := range stats.FleetTimings {
		response.FleetTimings = append(response.FleetTimings, &controlv1.FleetTimingTotal{
			Tier: total.Tier, Provider: total.Provider, Day: total.Day,
			Kind: string(total.Kind), Duration: durationSummaryToProto(total.Duration),
		})
	}
	for _, effectiveness := range stats.Routing {
		selections := make([]*controlv1.RoutingSelection, 0, len(effectiveness.Selections))
		for _, selection := range effectiveness.Selections {
			selections = append(selections, &controlv1.RoutingSelection{
				Tier: selection.Tier, Provider: selection.Provider, Jobs: selection.Jobs,
			})
		}
		response.RoutingEffectiveness = append(response.RoutingEffectiveness, &controlv1.RoutingEffectiveness{
			Route: effectiveness.Route, RequiredLabel: effectiveness.RequiredLabel,
			Currency: effectiveness.Currency, Decisions: effectiveness.Decisions,
			Completed: effectiveness.Completed, FallbackDecisions: effectiveness.FallbackDecisions,
			HistoryDecisions: effectiveness.HistoryDecisions, IdleDecisions: effectiveness.IdleDecisions,
			DeferredDecisions: effectiveness.DeferredDecisions, P95Hits: effectiveness.P95Hits,
			P95Misses: effectiveness.P95Misses, EstimatedSelectedNanos: effectiveness.EstimatedSelectedNanos,
			EstimatedFallbackNanos: effectiveness.EstimatedFallbackNanos,
			EstimatedSavingsNanos:  effectiveness.EstimatedSavingsNanos,
			ActualDirectNanos:      effectiveness.ActualDirectNanos,
			ActualUnknownEntries:   effectiveness.ActualUnknownEntries, Selections: selections,
		})
	}
	return connect.NewResponse(response), nil
}

func filterWorkerViews(workers []WorkerView, tier, provider string) []WorkerView {
	out := make([]WorkerView, 0, len(workers))
	for _, worker := range workers {
		if tier != "" && worker.Tier != tier {
			continue
		}
		if provider != "" && worker.ProviderName != provider {
			continue
		}
		out = append(out, worker)
	}
	return out
}

func validProtoTime(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, err
	}
	return value.AsTime(), nil
}

func jobToProto(job storage.Job) *controlv1.Job {
	return &controlv1.Job{
		Id: job.ID, Source: job.Source, Handle: job.Handle, ForgejoJobId: job.ForgejoJobID,
		Attempt: job.Attempt, RepositoryId: job.RepositoryID, Repository: job.Repository,
		Workflow: job.Workflow, WorkflowFile: job.WorkflowFile, JobName: job.JobName,
		IdentityQuality: string(job.IdentityQuality), Tier: job.Tier, Provider: job.Provider,
		Driver: job.Driver, Status: string(job.Status), Conclusion: job.Conclusion,
		InfrastructureFailure: job.InfrastructureFailure, FirstSeenAt: tsOrNil(job.FirstSeenAt),
		QueuedAt: tsOrNil(job.QueuedAt), DispatchedAt: tsOrNil(job.DispatchedAt),
		RunnerStartedAt: tsOrNil(job.RunnerStartedAt), RunnerFinishedAt: tsOrNil(job.RunnerFinishedAt),
		CompletedAt: tsOrNil(job.CompletedAt),
	}
}

func statisticsGroupToProto(group storage.StatisticsGroup) *controlv1.StatisticsGroup {
	directCosts := make([]*controlv1.CostTotal, 0, len(group.DirectCosts))
	for _, total := range group.DirectCosts {
		directCosts = append(directCosts, &controlv1.CostTotal{
			Kind: string(total.Kind), Currency: total.Currency, Nanos: total.Nanos,
			Entries: total.Entries, UnknownEntries: total.UnknownEntries, Estimated: total.Estimated,
		})
	}
	return &controlv1.StatisticsGroup{
		Key: &controlv1.StatisticsKey{
			Source: group.Key.Source, RepositoryId: group.Key.RepositoryID,
			Repository: group.Key.Repository, Workflow: group.Key.Workflow,
			Tier: group.Key.Tier, Provider: group.Key.Provider, Day: group.Key.Day,
		},
		Jobs: group.Jobs, Completed: group.Completed, Succeeded: group.Succeeded,
		Failed: group.Failed, Cancelled: group.Cancelled, Skipped: group.Skipped,
		InfraFailed: group.InfraFailed, Interrupted: group.Interrupted,
		InProgress: group.InProgress, PricedJobs: group.PricedJobs,
		UnpricedJobs: group.UnpricedJobs, QueueDuration: durationSummaryToProto(group.QueueDuration),
		DispatchDuration: durationSummaryToProto(group.DispatchDuration),
		RunDuration:      durationSummaryToProto(group.RunDuration), DirectCosts: directCosts,
	}
}

func durationSummaryToProto(summary storage.DurationSummary) *controlv1.DurationSummary {
	return &controlv1.DurationSummary{
		Count: summary.Count, Total: durationpb.New(summary.Total), Min: durationpb.New(summary.Min),
		Max: durationpb.New(summary.Max), P50: durationpb.New(summary.P50), P95: durationpb.New(summary.P95),
	}
}

// auditCaller builds a short, log-safe identity string from the Connect
// request: the peer address always, plus a "token" marker when the
// Authorization header carries a bearer token (we don't decode it — the
// header's presence is the signal). Format example: "peer=127.0.0.1:54312"
// or "peer=10.0.0.5:60000 token". Loopback peers also get the explicit
// peer= prefix so the operator can distinguish "someone hit /healthz over
// loopback" from "nothing set this at all".
func auditCaller[T any](req *connect.Request[T]) string {
	parts := make([]string, 0, 2)
	if p := req.Peer().Addr; p != "" {
		parts = append(parts, "peer="+p)
	}
	if h := req.Header().Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		parts = append(parts, "token")
	}
	return strings.Join(parts, " ")
}

// tsOrNil emits a Timestamp only for non-zero times; zero stays nil so the
// wire form omits the field instead of advertising 1970-01-01.
func tsOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
