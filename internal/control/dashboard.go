package control

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/websocket"

	"github.com/hstern/fj-bellows/internal/control/events"
	"github.com/hstern/fj-bellows/internal/storage"
)

const (
	dashboardDefaultWindow = 24 * time.Hour
	dashboardMaxWindow     = 90 * 24 * time.Hour
	dashboardJobLimit      = 100
)

//go:embed dashboard/*
var dashboardFiles embed.FS

type dashboardResponse struct {
	GeneratedAt time.Time           `json:"generated_at"`
	Window      string              `json:"window"`
	Health      dashboardHealth     `json:"health"`
	Summary     dashboardSummary    `json:"summary"`
	Workers     []dashboardWorker   `json:"workers"`
	Queue       []dashboardQueueJob `json:"queue"`
	Jobs        []dashboardJob      `json:"jobs"`
	Statistics  dashboardStatistics `json:"statistics"`
	Warnings    []string            `json:"warnings,omitempty"`
}

type dashboardHealth struct {
	Healthy                bool   `json:"healthy"`
	Paused                 bool   `json:"paused"`
	DatabaseHealthy        bool   `json:"database_healthy"`
	RoutingHealthy         bool   `json:"routing_healthy"`
	RoutingDegradedPricing bool   `json:"routing_degraded_pricing"`
	LastTickAt             string `json:"last_tick_at,omitempty"`
	LastForgejoPollAt      string `json:"last_forgejo_poll_at,omitempty"`
	RoutingLastPollAt      string `json:"routing_last_poll_at,omitempty"`
	DatabaseError          string `json:"database_error,omitempty"`
	RoutingError           string `json:"routing_error,omitempty"`
}

type dashboardSummary struct {
	Workers            int   `json:"workers"`
	IdleWorkers        int   `json:"idle_workers"`
	BusyWorkers        int   `json:"busy_workers"`
	Transitional       int   `json:"transitional_workers"`
	QueuedJobs         int   `json:"queued_jobs"`
	OptimizationQueued int   `json:"optimization_queued_jobs"`
	InProgressJobs     int64 `json:"in_progress_jobs"`
	CompletedJobs      int64 `json:"completed_jobs"`
	SucceededJobs      int64 `json:"succeeded_jobs"`
	FailedJobs         int64 `json:"failed_jobs"`
}

type dashboardWorker struct {
	Tier           string `json:"tier"`
	Provider       string `json:"provider"`
	Driver         string `json:"driver"`
	InstanceID     string `json:"instance_id"`
	State          string `json:"state"`
	IP             string `json:"ip"`
	VPCIP          string `json:"vpc_ip"`
	CurrentJob     string `json:"current_job"`
	BillingModel   string `json:"billing_model"`
	CreatedAt      string `json:"created_at,omitempty"`
	LastBusy       string `json:"last_busy,omitempty"`
	PaidHourEndAt  string `json:"paid_hour_end_at,omitempty"`
	ReapEligibleAt string `json:"reap_eligible_at,omitempty"`
}

type dashboardQueueJob struct {
	Handle                    string `json:"handle"`
	Repository                string `json:"repository"`
	Workflow                  string `json:"workflow"`
	JobName                   string `json:"job_name"`
	Tier                      string `json:"tier"`
	Provider                  string `json:"provider"`
	Route                     string `json:"route"`
	RoutingState              string `json:"routing_state"`
	SelectionReason           string `json:"selection_reason"`
	OptimizationQueued        bool   `json:"optimization_queued"`
	SelectedWorkerID          string `json:"selected_worker_id"`
	OptimizationQueuePosition int    `json:"optimization_queue_position"`
	OptimizationWait          int64  `json:"optimization_wait_ns"`
	PredictedP95              int64  `json:"predicted_p95_ns"`
	FirstSeenAt               string `json:"first_seen_at,omitempty"`
	ScheduledStartAt          string `json:"scheduled_start_at,omitempty"`
	ScheduledFinishAt         string `json:"scheduled_finish_at,omitempty"`
}

type dashboardJob struct {
	Handle                string `json:"handle"`
	Repository            string `json:"repository"`
	Workflow              string `json:"workflow"`
	JobName               string `json:"job_name"`
	Tier                  string `json:"tier"`
	Provider              string `json:"provider"`
	Status                string `json:"status"`
	InfrastructureFailure string `json:"infrastructure_failure,omitempty"`
	FirstSeenAt           string `json:"first_seen_at,omitempty"`
	CompletedAt           string `json:"completed_at,omitempty"`
	QueueDurationNS       int64  `json:"queue_duration_ns"`
	RunDurationNS         int64  `json:"run_duration_ns"`
}

type dashboardStatistics struct {
	QueueP95NS  int64              `json:"queue_p95_ns"`
	RunP95NS    int64              `json:"run_p95_ns"`
	DirectCosts []dashboardCost    `json:"direct_costs"`
	FleetCosts  []dashboardCost    `json:"fleet_costs"`
	Routing     []dashboardRouting `json:"routing"`
}

type dashboardCost struct {
	Scope          string `json:"scope"`
	Tier           string `json:"tier,omitempty"`
	Provider       string `json:"provider,omitempty"`
	Kind           string `json:"kind"`
	Currency       string `json:"currency"`
	Nanos          int64  `json:"nanos"`
	Entries        int64  `json:"entries"`
	UnknownEntries int64  `json:"unknown_entries"`
	Estimated      bool   `json:"estimated"`
}

type dashboardRouting struct {
	Route                  string `json:"route"`
	Currency               string `json:"currency"`
	Decisions              int64  `json:"decisions"`
	Completed              int64  `json:"completed"`
	FallbackDecisions      int64  `json:"fallback_decisions"`
	HistoryDecisions       int64  `json:"history_decisions"`
	IdleDecisions          int64  `json:"idle_decisions"`
	DeferredDecisions      int64  `json:"deferred_decisions"`
	P95Hits                int64  `json:"p95_hits"`
	P95Misses              int64  `json:"p95_misses"`
	EstimatedSelectedNanos int64  `json:"estimated_selected_nanos"`
	EstimatedSavingsNanos  int64  `json:"estimated_savings_nanos"`
	ActualDirectNanos      int64  `json:"actual_direct_nanos"`
	ActualUnknownEntries   int64  `json:"actual_unknown_entries"`
}

type dashboardEvent struct {
	Kind  string            `json:"kind"`
	At    time.Time         `json:"at"`
	Type  string            `json:"type,omitempty"`
	Attrs map[string]string `json:"attrs,omitempty"`
}

type dashboardQueueReporter interface {
	QueueSnapshot(context.Context, time.Time) ([]storage.QueueJob, error)
}

func newDashboardHandler(backend Backend, now func() time.Time) http.Handler {
	if now == nil {
		now = time.Now
	}
	assets, err := fs.Sub(dashboardFiles, "dashboard")
	if err != nil {
		panic(fmt.Sprintf("dashboard assets: %v", err))
	}
	files := http.FileServer(http.FS(assets))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /dashboard/api", func(w http.ResponseWriter, r *http.Request) {
		serveDashboardAPI(w, r, backend, now)
	})
	mux.Handle("GET /dashboard/ws", dashboardWebSocketHandler(backend, now))
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusTemporaryRedirect)
	})
	mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", files))
	return securityHeaders(mux)
}

func dashboardWebSocketHandler(backend Backend, now func() time.Time) http.Handler {
	return websocket.Server{
		Handshake: func(config *websocket.Config, request *http.Request) error {
			origin, err := websocket.Origin(config, request)
			if err != nil {
				return err
			}
			if origin != nil && !sameDashboardOrigin(origin, request) {
				return fmt.Errorf("websocket origin %q does not match host %q", origin.Host, request.Host)
			}
			for _, protocol := range config.Protocol {
				if protocol == "fjb-events-v1" {
					config.Protocol = []string{protocol}
					return nil
				}
			}
			config.Protocol = nil
			return nil
		},
		Handler: func(conn *websocket.Conn) {
			serveDashboardWebSocket(conn, backend, now)
		},
	}
}

func serveDashboardWebSocket(conn *websocket.Conn, backend Backend, now func() time.Time) {
	defer func() { _ = conn.Close() }()
	stream, cancel := backend.Subscribe()
	defer cancel()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			var ignored any
			if websocket.JSON.Receive(conn, &ignored) != nil {
				return
			}
		}
	}()
	if err := websocket.JSON.Send(conn, dashboardEvent{Kind: "connected", At: now().UTC()}); err != nil {
		return
	}
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-closed:
			return
		case event, ok := <-stream:
			if !ok {
				return
			}
			if err := websocket.JSON.Send(conn, dashboardEventFromBus(event)); err != nil {
				return
			}
		case at := <-heartbeat.C:
			if err := websocket.JSON.Send(conn, dashboardEvent{Kind: "heartbeat", At: at.UTC()}); err != nil {
				return
			}
		}
	}
}

func dashboardEventFromBus(event events.Event) dashboardEvent {
	return dashboardEvent{Kind: "event", At: event.At.UTC(), Type: event.Type, Attrs: event.Attrs}
}

func sameDashboardOrigin(origin *url.URL, request *http.Request) bool {
	return (origin.Scheme == "http" || origin.Scheme == "https") &&
		strings.EqualFold(origin.Host, request.Host)
}

func serveDashboardAPI(w http.ResponseWriter, r *http.Request, backend Backend, now func() time.Time) {
	current := now().UTC()
	window, err := dashboardWindow(r.URL.Query().Get("window"))
	if err != nil {
		writeDashboardError(w, http.StatusBadRequest, err)
		return
	}
	response := buildDashboardResponse(r.Context(), backend, current, window)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		return
	}
}

func buildDashboardResponse(ctx context.Context, backend Backend, now time.Time, window time.Duration) dashboardResponse {
	health := backend.Health(ctx)
	response := dashboardResponse{
		GeneratedAt: now,
		Window:      window.String(),
		Health: dashboardHealth{
			Healthy: health.Healthy, Paused: health.Paused,
			DatabaseHealthy: health.DatabaseHealthy, RoutingHealthy: health.RoutingHealthy,
			RoutingDegradedPricing: health.RoutingDegradedPricing,
			LastTickAt:             formatDashboardTime(health.LastTickAt), LastForgejoPollAt: formatDashboardTime(health.LastForgejoPollAt),
			RoutingLastPollAt: formatDashboardTime(health.RoutingLastPollAt),
			DatabaseError:     health.DatabaseLastError, RoutingError: health.RoutingLastError,
		},
		Workers: []dashboardWorker{}, Queue: []dashboardQueueJob{}, Jobs: []dashboardJob{},
		Statistics: dashboardStatistics{DirectCosts: []dashboardCost{}, FleetCosts: []dashboardCost{}, Routing: []dashboardRouting{}},
	}
	for _, worker := range backend.PoolSnapshot() {
		response.Workers = append(response.Workers, dashboardWorker{
			Tier: worker.Tier, Provider: worker.ProviderName, Driver: worker.Driver,
			InstanceID: worker.InstanceID, State: worker.State, IP: worker.IP, VPCIP: worker.VPCIP,
			CurrentJob: worker.CurrentJob, BillingModel: worker.BillingModel,
			CreatedAt: formatDashboardTime(worker.CreatedAt), LastBusy: formatDashboardTime(worker.LastBusy),
			PaidHourEndAt: formatDashboardTime(worker.PaidHourEndAt), ReapEligibleAt: formatDashboardTime(worker.ReapEligibleAt),
		})
		switch worker.State {
		case "idle":
			response.Summary.IdleWorkers++
		case "busy":
			response.Summary.BusyWorkers++
		default:
			response.Summary.Transitional++
		}
	}
	response.Summary.Workers = len(response.Workers)

	loadDashboardQueue(ctx, backend, now, &response)

	if reporter, ok := backend.(interface {
		JobHistory(context.Context, storage.HistoryFilter) (storage.JobPage, error)
	}); ok {
		page, err := reporter.JobHistory(ctx, storage.HistoryFilter{
			From: now.Add(-window), To: now.Add(time.Nanosecond), Limit: dashboardJobLimit,
		})
		if err != nil {
			response.Warnings = append(response.Warnings, "jobs: "+err.Error())
		} else {
			for _, job := range page.Jobs {
				response.Jobs = append(response.Jobs, dashboardJobFromStorage(job, now))
			}
		}
	}

	if reporter, ok := backend.(interface {
		Statistics(context.Context, storage.StatisticsFilter) (storage.Statistics, error)
	}); ok {
		stats, err := reporter.Statistics(ctx, storage.StatisticsFilter{
			From: now.Add(-window), To: now.Add(time.Nanosecond), GroupBy: storage.GroupNone,
		})
		if err != nil {
			response.Warnings = append(response.Warnings, "statistics: "+err.Error())
		} else {
			response.Statistics = dashboardStatisticsFromStorage(stats)
			for _, group := range stats.Groups {
				response.Summary.InProgressJobs += group.InProgress
				response.Summary.CompletedJobs += group.Completed
				response.Summary.SucceededJobs += group.Succeeded
				response.Summary.FailedJobs += group.Failed + group.InfraFailed + group.Interrupted
			}
		}
	}
	return response
}

func loadDashboardQueue(ctx context.Context, backend Backend, now time.Time, response *dashboardResponse) {
	reporter, ok := backend.(dashboardQueueReporter)
	if !ok {
		response.Warnings = append(response.Warnings, "durable queue snapshot is unavailable")
		return
	}
	queue, err := reporter.QueueSnapshot(ctx, now)
	if err != nil {
		response.Warnings = append(response.Warnings, "queue: "+err.Error())
		return
	}
	for _, job := range queue {
		response.Queue = append(response.Queue, dashboardQueueJob{
			Handle: job.Handle, Repository: job.Repository, Workflow: job.Workflow,
			JobName: job.JobName, Tier: job.Tier, Provider: job.Provider, Route: job.Route,
			RoutingState: job.RoutingState, SelectionReason: job.SelectionReason,
			OptimizationQueued: job.OptimizationQueued, SelectedWorkerID: job.SelectedWorkerID,
			OptimizationQueuePosition: job.OptimizationQueuePosition,
			OptimizationWait:          job.OptimizationWait, PredictedP95: job.PredictedP95,
			FirstSeenAt:       formatDashboardTime(job.FirstSeenAt),
			ScheduledStartAt:  formatDashboardTime(job.ScheduledStartAt),
			ScheduledFinishAt: formatDashboardTime(job.ScheduledFinishAt),
		})
		if job.OptimizationQueued {
			response.Summary.OptimizationQueued++
		}
	}
	response.Summary.QueuedJobs = len(queue)
}

func dashboardJobFromStorage(job storage.Job, now time.Time) dashboardJob {
	workflow := job.WorkflowFile
	if workflow == "" {
		workflow = job.Workflow
	}
	queueEnd := job.DispatchedAt
	if queueEnd.IsZero() {
		queueEnd = now
	}
	runEnd := job.RunnerFinishedAt
	if runEnd.IsZero() && !job.RunnerStartedAt.IsZero() {
		runEnd = now
	}
	return dashboardJob{
		Handle: job.Handle, Repository: job.Repository, Workflow: workflow, JobName: job.JobName,
		Tier: job.Tier, Provider: job.Provider, Status: string(job.Status),
		InfrastructureFailure: job.InfrastructureFailure,
		FirstSeenAt:           formatDashboardTime(job.FirstSeenAt), CompletedAt: formatDashboardTime(job.CompletedAt),
		QueueDurationNS: durationBetween(job.FirstSeenAt, queueEnd),
		RunDurationNS:   durationBetween(job.RunnerStartedAt, runEnd),
	}
}

func dashboardStatisticsFromStorage(stats storage.Statistics) dashboardStatistics {
	result := dashboardStatistics{
		DirectCosts: []dashboardCost{}, FleetCosts: []dashboardCost{}, Routing: []dashboardRouting{},
	}
	for _, group := range stats.Groups {
		result.QueueP95NS = max(result.QueueP95NS, int64(group.QueueDuration.P95))
		result.RunP95NS = max(result.RunP95NS, int64(group.RunDuration.P95))
		for _, cost := range group.DirectCosts {
			result.DirectCosts = append(result.DirectCosts, dashboardCost{
				Scope: "jobs", Kind: string(cost.Kind), Currency: cost.Currency,
				Nanos: cost.Nanos, Entries: cost.Entries, UnknownEntries: cost.UnknownEntries,
				Estimated: cost.Estimated,
			})
		}
	}
	for _, cost := range stats.FleetCosts {
		result.FleetCosts = append(result.FleetCosts, dashboardCost{
			Scope: "fleet", Tier: cost.Tier, Provider: cost.Provider,
			Kind: string(cost.Kind), Currency: cost.Currency, Nanos: cost.Nanos,
			Entries: cost.Entries, UnknownEntries: cost.UnknownEntries, Estimated: cost.Estimated,
		})
	}
	for _, route := range stats.Routing {
		result.Routing = append(result.Routing, dashboardRouting{
			Route: route.Route, Currency: route.Currency, Decisions: route.Decisions,
			Completed: route.Completed, FallbackDecisions: route.FallbackDecisions,
			HistoryDecisions: route.HistoryDecisions, IdleDecisions: route.IdleDecisions,
			DeferredDecisions: route.DeferredDecisions, P95Hits: route.P95Hits, P95Misses: route.P95Misses,
			EstimatedSelectedNanos: route.EstimatedSelectedNanos,
			EstimatedSavingsNanos:  route.EstimatedSavingsNanos,
			ActualDirectNanos:      route.ActualDirectNanos, ActualUnknownEntries: route.ActualUnknownEntries,
		})
	}
	return result
}

func dashboardWindow(raw string) (time.Duration, error) {
	if raw == "" {
		return dashboardDefaultWindow, nil
	}
	window, err := time.ParseDuration(raw)
	if err != nil || window <= 0 || window > dashboardMaxWindow {
		return 0, fmt.Errorf("window must be a positive Go duration no greater than %s", dashboardMaxWindow)
	}
	return window, nil
}

func durationBetween(start, end time.Time) int64 {
	if start.IsZero() || !end.After(start) {
		return 0
	}
	return int64(end.Sub(start))
}

func formatDashboardTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func writeDashboardError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}
