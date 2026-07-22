package forgejo

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	metadataToken          = "metadata-secret"
	metadataRepositoryPath = "/api/v1/repositories/42"
)

func TestWaitingJobsBareArray(t *testing.T) {
	// The Forgejo 11.x/12.x /actions/runners/jobs endpoint returns a bare JSON
	// array of ActionRunJob and requires a non-empty labels query.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/actions/runners/jobs") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("labels"); got != "docker,linux" {
			t.Errorf("labels query = %q, want docker,linux", got)
		}
		if got := r.Header.Get("Authorization"); got != "token secret" {
			t.Errorf("auth header = %q", got)
		}
		_, _ = io.WriteString(w, `[{"id":2,"name":"hello","runs_on":["docker","linux"],"status":"waiting","task_id":0}]`)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/example", "secret", "docker", "linux")
	jobs, err := c.WaitingJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != 2 || jobs[0].Name != "hello" {
		t.Fatalf("jobs = %+v", jobs)
	}
	if len(jobs[0].Labels) != 2 || jobs[0].Labels[0] != "docker" {
		t.Fatalf("labels = %+v", jobs[0].Labels)
	}
}

func TestWaitingJobsWrappedTolerated(t *testing.T) {
	// Future-proof: if a Forgejo version wraps the response in {"jobs":[...]},
	// the client still decodes it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"jobs":[{"id":1,"handle":"h1","runs_on":["docker"]}]}`)
	}))
	defer srv.Close()

	c := New(srv.URL, "repos/o/r", "t", "docker")
	jobs, err := c.WaitingJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Handle != "h1" {
		t.Fatalf("jobs = %+v", jobs)
	}
}

func TestWaitingJobsNullResponse(t *testing.T) {
	// Forgejo returns the literal `null` when no jobs match the labels filter.
	// The client treats that as an empty queue, not an error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `null`)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/x", "t", "docker")
	jobs, err := c.WaitingJobs(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("jobs = %+v, want empty", jobs)
	}
}

func TestWaitingJobsNoLabels(t *testing.T) {
	// With no labels configured the client omits the query parameter (and the
	// server typically returns null in that case).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("labels"); got != "" {
			t.Errorf("labels query should be empty, got %q", got)
		}
		_, _ = io.WriteString(w, `null`)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/x", "t")
	if _, err := c.WaitingJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterEphemeral(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["ephemeral"] != true {
			t.Errorf("ephemeral not set: %+v", body)
		}
		_, _ = io.WriteString(w, `{"uuid":"u-1","token":"tok-1"}`)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/example", "t", "docker")
	reg, err := c.RegisterEphemeral(context.Background(), "runner-x", []string{labelUbuntu})
	if err != nil {
		t.Fatal(err)
	}
	if reg.UUID != "u-1" || reg.Token != "tok-1" {
		t.Fatalf("reg = %+v", reg)
	}
}

func TestRegisterEphemeralMissingFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/x", "t")
	if _, err := c.RegisterEphemeral(context.Background(), "r", nil); err == nil {
		t.Fatal("expected error for missing uuid/token")
	}
}

func TestRegisterEphemeralForgejo12(t *testing.T) {
	// Forgejo 12 has no POST /actions/runners endpoint and returns 404. The
	// client surfaces that with a "requires Forgejo >= 15" hint so the
	// operator sees a clear diagnostic.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/x", "t")
	_, err := c.RegisterEphemeral(context.Background(), "r", nil)
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if !strings.Contains(err.Error(), "Forgejo >= 15") {
		t.Fatalf("error should mention Forgejo >= 15: %v", err)
	}
}

func TestListRunners(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/actions/runners") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"runners":[{"id":7,"uuid":"u-7","name":"fj-bellows-abcd","status":"offline"}]}`)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/example", "t")
	runners, err := c.ListRunners(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runners) != 1 || runners[0].ID != 7 || runners[0].Name != "fj-bellows-abcd" {
		t.Fatalf("runners = %+v", runners)
	}
}

func TestListRunnersForgejo12(t *testing.T) {
	// Forgejo <= 12 lacks GET /actions/runners and returns 404. The client
	// translates that into an empty list so the orchestrator's zombie-runner
	// reaper does not flood the log on every poll.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/x", "t")
	runners, err := c.ListRunners(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(runners) != 0 {
		t.Fatalf("runners = %+v, want empty", runners)
	}
}

func TestDeleteRunner(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/example", "t")
	if err := c.DeleteRunner(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || !strings.HasSuffix(gotPath, "/actions/runners/7") {
		t.Errorf("DeleteRunner hit %s %s", gotMethod, gotPath)
	}
}

func TestDoNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	c := New(srv.URL, "orgs/x", "t")
	if _, err := c.WaitingJobs(context.Background()); err == nil {
		t.Fatal("expected error on 403")
	}
}

//nolint:gocyclo // This integration-style handler validates every request and all enriched response fields.
func TestJobMetadataEnrichesRepositoryJobAndRun(t *testing.T) {
	expectedPaths := []string{
		metadataRepositoryPath,
		"/api/v1/repos/example/project/actions/jobs/7",
		"/api/v1/repos/example/project/actions/runs/99",
	}
	var requestIndex atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := int(requestIndex.Add(1)) - 1
		if index >= len(expectedPaths) {
			t.Errorf("unexpected extra request: %s %s", r.Method, r.URL.EscapedPath())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("request %d method = %s, want GET", index, r.Method)
		}
		if r.URL.EscapedPath() != expectedPaths[index] {
			t.Errorf("request %d path = %q, want %q", index, r.URL.EscapedPath(), expectedPaths[index])
		}
		if got := r.Header.Get("Authorization"); got != "token "+metadataToken {
			t.Errorf("request %d authorization = %q", index, got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("request %d accept = %q", index, got)
		}
		switch index {
		case 0:
			_, _ = io.WriteString(w, `{"full_name":"example/project"}`)
		case 1:
			_, _ = io.WriteString(w, `{"run_id":99,"commit_sha":"job-sha","status":"completed","conclusion":"success","queued_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","completed_at":"2026-07-22T10:06:00Z"}`)
		case 2:
			_, _ = io.WriteString(w, `{"workflow_id":".forgejo/workflows/ci.yml","ref":"refs/heads/main","event":"push","head_sha":"run-sha"}`)
		}
	}))
	defer srv.Close()

	client := New(srv.URL, "orgs/example", metadataToken)
	meta, err := client.JobMetadata(context.Background(), WaitingJob{ID: 7, RepoID: 42})
	if err != nil {
		t.Fatalf("JobMetadata: %v", err)
	}
	if int(requestIndex.Load()) != len(expectedPaths) {
		t.Fatalf("requests = %d, want %d", requestIndex.Load(), len(expectedPaths))
	}
	if meta.Repository != "example/project" || meta.WorkflowID != ".forgejo/workflows/ci.yml" ||
		meta.RunID != 99 || meta.Ref != "refs/heads/main" || meta.Event != "push" ||
		meta.CommitSHA != "job-sha" || meta.Status != "completed" || meta.Conclusion != "success" ||
		meta.QueuedAt != "2026-07-22T10:00:00Z" || meta.StartedAt != "2026-07-22T10:01:00Z" ||
		meta.CompletedAt != "2026-07-22T10:06:00Z" {
		t.Fatalf("metadata = %#v", meta)
	}
}

func TestJobMetadataFallsBackToWaitingJobRunID(t *testing.T) {
	var runPath atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "token "+metadataToken {
			t.Errorf("authorization = %q", got)
		}
		switch r.URL.EscapedPath() {
		case metadataRepositoryPath:
			_, _ = io.WriteString(w, `{"full_name":"example/project"}`)
		case "/api/v1/repos/example/project/actions/jobs/7":
			_, _ = io.WriteString(w, `{"run_id":0,"commit_sha":""}`)
		case "/api/v1/repos/example/project/actions/runs/123":
			runPath.Store(r.URL.EscapedPath())
			_, _ = io.WriteString(w, `{"workflow_id":"fallback.yml","head_sha":"fallback-sha"}`)
		default:
			t.Errorf("unexpected path %q", r.URL.EscapedPath())
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(srv.URL, "orgs/example", metadataToken)
	meta, err := client.JobMetadata(context.Background(), WaitingJob{ID: 7, RepoID: 42, RunID: 123})
	if err != nil {
		t.Fatalf("JobMetadata: %v", err)
	}
	if meta.RunID != 123 || meta.WorkflowID != "fallback.yml" || meta.CommitSHA != "fallback-sha" {
		t.Fatalf("metadata = %#v", meta)
	}
	if got, _ := runPath.Load().(string); got != "/api/v1/repos/example/project/actions/runs/123" {
		t.Fatalf("run lookup path = %q", got)
	}
}

func TestJobMetadataUnavailable(t *testing.T) {
	t.Run("missing repo ID does not perform lookup", func(t *testing.T) {
		var requests atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}))
		defer srv.Close()

		client := New(srv.URL, "orgs/example", metadataToken)
		_, err := client.JobMetadata(context.Background(), WaitingJob{ID: 7})
		if !errors.Is(err, ErrMetadataUnavailable) {
			t.Fatalf("error = %v, want ErrMetadataUnavailable", err)
		}
		if requests.Load() != 0 {
			t.Fatalf("requests = %d, want 0", requests.Load())
		}
	})

	for _, test := range []struct {
		name      string
		failPath  string
		wantStage string
	}{
		{name: "repository lookup failure", failPath: metadataRepositoryPath, wantStage: "repository lookup"},
		{name: "job lookup failure", failPath: "/api/v1/repos/example/project/actions/jobs/7", wantStage: "job lookup"},
	} {
		t.Run(test.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() == test.failPath {
					http.Error(w, "lookup failed", http.StatusNotFound)
					return
				}
				if r.URL.EscapedPath() == metadataRepositoryPath {
					_, _ = io.WriteString(w, `{"full_name":"example/project"}`)
					return
				}
				t.Errorf("unexpected path %q", r.URL.EscapedPath())
				http.NotFound(w, r)
			}))
			defer srv.Close()

			client := New(srv.URL, "orgs/example", metadataToken)
			_, err := client.JobMetadata(context.Background(), WaitingJob{ID: 7, RepoID: 42})
			if !errors.Is(err, ErrMetadataUnavailable) {
				t.Fatalf("error = %v, want ErrMetadataUnavailable", err)
			}
			if !strings.Contains(err.Error(), test.wantStage) {
				t.Fatalf("error = %v, want stage %q", err, test.wantStage)
			}
		})
	}
}
