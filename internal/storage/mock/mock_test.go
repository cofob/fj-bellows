package mock

import (
	"context"
	"sync"
	"testing"

	"github.com/hstern/fj-bellows/internal/storage"
)

func TestStoreDelegatesAndRecordsConcurrentCalls(t *testing.T) {
	store := &Store{
		UpsertJobFn: func(_ context.Context, job storage.Job) (storage.Job, error) {
			job.ID = 7
			return job, nil
		},
	}
	const count = 20
	var wait sync.WaitGroup
	for range count {
		wait.Go(func() {
			job, err := store.UpsertJob(context.Background(), storage.Job{Handle: "job"})
			if err != nil || job.ID != 7 {
				t.Errorf("UpsertJob() = %+v, %v", job, err)
			}
		})
	}
	wait.Wait()
	if got := store.CallCount("UpsertJob"); got != count {
		t.Fatalf("CallCount(UpsertJob) = %d, want %d", got, count)
	}
	if got := len(store.Calls()); got != count {
		t.Fatalf("len(Calls()) = %d, want %d", got, count)
	}
}
