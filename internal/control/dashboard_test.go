package control_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"

	"github.com/hstern/fj-bellows/internal/control"
	"github.com/hstern/fj-bellows/internal/control/events"
	mockctl "github.com/hstern/fj-bellows/internal/control/mock"
	"github.com/hstern/fj-bellows/internal/storage"
)

const dashboardEventProtocol = "fjb-events-v1"

type dashboardBackend struct {
	*mockctl.Backend
	queue []storage.QueueJob
	jobs  storage.JobPage
	stats storage.Statistics
}

func (b *dashboardBackend) QueueSnapshot(context.Context, time.Time) ([]storage.QueueJob, error) {
	return append([]storage.QueueJob(nil), b.queue...), nil
}

func (b *dashboardBackend) JobHistory(context.Context, storage.HistoryFilter) (storage.JobPage, error) {
	return b.jobs, nil
}

func (b *dashboardBackend) Statistics(context.Context, storage.StatisticsFilter) (storage.Statistics, error) {
	return b.stats, nil
}

func TestDashboardServesLiveFleetQueueAndStatistics(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	base := &mockctl.Backend{}
	base.SetHealth(func(context.Context) control.HealthStatus {
		return control.HealthStatus{Healthy: true, DatabaseHealthy: true, RoutingHealthy: true, LastTickAt: now}
	})
	base.SetPoolSnapshot(func() []control.WorkerView {
		return []control.WorkerView{{
			Tier: "long", ProviderName: "hz", Driver: "hetzner", InstanceID: testInstance,
			State: "busy", CurrentJob: "job-1", CreatedAt: now.Add(-10 * time.Minute),
			ReapEligibleAt: now.Add(45 * time.Minute), BillingModel: "hourly_round_up",
		}}
	})
	backend := &dashboardBackend{
		Backend: base,
		queue: []storage.QueueJob{{
			ID: 2, Handle: "job-2", JobName: "test", Tier: "long", Route: "amd64",
			OptimizationQueued: true, OptimizationQueuePosition: 1, SelectedWorkerID: testInstance,
			FirstSeenAt: now.Add(-time.Minute), PredictedP95: int64(5 * time.Minute),
		}},
		jobs: storage.JobPage{Jobs: []storage.Job{{
			Handle: "job-1", JobName: "build", Tier: "long", Provider: "hz",
			Status: storage.JobRunning, FirstSeenAt: now.Add(-20 * time.Minute),
			DispatchedAt: now.Add(-19 * time.Minute), RunnerStartedAt: now.Add(-18 * time.Minute),
		}}},
		stats: storage.Statistics{Groups: []storage.StatisticsGroup{{
			Jobs: 3, Completed: 2, Succeeded: 2, InProgress: 1,
			QueueDuration: storage.DurationSummary{P95: time.Minute},
			RunDuration:   storage.DurationSummary{P95: 15 * time.Minute},
		}}},
	}
	hs, _ := newTestServer(t, backend)

	resp, err := dashboardGET(t, hs.Client(), hs.URL+"/dashboard/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Live workers") ||
		resp.Header.Get("Content-Security-Policy") == "" {
		t.Fatalf("dashboard shell status=%d headers=%v body=%q", resp.StatusCode, resp.Header, body)
	}

	resp, err = dashboardGET(t, hs.Client(), hs.URL+"/dashboard/api?window=24h")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var snapshot struct {
		Summary struct {
			Workers            int `json:"workers"`
			QueuedJobs         int `json:"queued_jobs"`
			OptimizationQueued int `json:"optimization_queued_jobs"`
		} `json:"summary"`
		Queue      []storage.QueueJob `json:"queue"`
		Statistics struct {
			RunP95 int64 `json:"run_p95_ns"`
		} `json:"statistics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Summary.Workers != 1 || snapshot.Summary.QueuedJobs != 1 ||
		snapshot.Summary.OptimizationQueued != 1 || len(snapshot.Queue) != 1 ||
		snapshot.Statistics.RunP95 != int64(15*time.Minute) {
		t.Fatalf("dashboard snapshot = %+v", snapshot)
	}
}

func TestDashboardAPIValidatesWindow(t *testing.T) {
	backend := &mockctl.Backend{}
	hs, _ := newTestServer(t, backend)
	resp, err := dashboardGET(t, hs.Client(), hs.URL+"/dashboard/api?window=forever")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestDashboardShellOpenButAPIRequiresConfiguredToken(t *testing.T) {
	backend := &mockctl.Backend{}
	hs, _ := newAuthedServer(t, backend, "dashboard-secret")

	resp, err := dashboardGET(t, hs.Client(), hs.URL+"/dashboard/app.js")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("static shell status = %d, want 200", resp.StatusCode)
	}

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, hs.URL+"/dashboard/api", nil)
	resp, err = hs.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated API status = %d, want 401", resp.StatusCode)
	}

	req, _ = http.NewRequestWithContext(t.Context(), http.MethodGet, hs.URL+"/dashboard/api", nil)
	req.Header.Set("Authorization", "Bearer dashboard-secret")
	resp, err = hs.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated API status = %d, want 200", resp.StatusCode)
	}
}

func TestDashboardWebSocketForwardsFleetEvents(t *testing.T) {
	bus := events.New()
	backend := &mockctl.Backend{}
	backend.SetSubscribe(bus.Subscribe)
	server := control.NewServer("127.0.0.1:0", backend, nil)
	hs := httptest.NewServer(server.Handler())
	t.Cleanup(hs.Close)
	config, err := websocket.NewConfig("ws"+strings.TrimPrefix(hs.URL, "http")+"/dashboard/ws", hs.URL)
	if err != nil {
		t.Fatal(err)
	}
	config.Protocol = []string{dashboardEventProtocol}
	conn, err := websocket.DialConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	var message struct {
		Kind  string            `json:"kind"`
		Type  string            `json:"type"`
		Attrs map[string]string `json:"attrs"`
	}
	if err := websocket.JSON.Receive(conn, &message); err != nil || message.Kind != "connected" {
		t.Fatalf("connected event = %+v, %v", message, err)
	}
	bus.Publish(events.Event{At: time.Now(), Type: "worker_busy", Attrs: map[string]string{"id": testInstance}})
	if err := websocket.JSON.Receive(conn, &message); err != nil {
		t.Fatal(err)
	}
	if message.Kind != "event" || message.Type != "worker_busy" || message.Attrs["id"] != testInstance {
		t.Fatalf("fleet event = %+v", message)
	}
}

func TestDashboardWebSocketAcceptsBearerSubprotocol(t *testing.T) {
	const secret = "dashboard-secret"
	backend := &mockctl.Backend{}
	server := control.NewServer("127.0.0.1:0", backend, nil, control.WithBearerToken(secret))
	hs := httptest.NewServer(server.Handler())
	t.Cleanup(hs.Close)
	endpoint := "ws" + strings.TrimPrefix(hs.URL, "http") + "/dashboard/ws"

	unauthorized, err := websocket.NewConfig(endpoint, hs.URL)
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Protocol = []string{dashboardEventProtocol}
	if conn, dialErr := websocket.DialConfig(unauthorized); dialErr == nil {
		_ = conn.Close()
		t.Fatal("websocket without bearer subprotocol unexpectedly connected")
	}

	authorized, err := websocket.NewConfig(endpoint, hs.URL)
	if err != nil {
		t.Fatal(err)
	}
	authorized.Protocol = []string{dashboardEventProtocol, "fjb-bearer.ZGFzaGJvYXJkLXNlY3JldA"}
	conn, err := websocket.DialConfig(authorized)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if conn.Config().Protocol[0] != dashboardEventProtocol {
		t.Fatalf("selected protocol = %v", conn.Config().Protocol)
	}
}

func dashboardGET(t *testing.T, client *http.Client, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}
