package mock_test

import (
	"context"
	"sync"
	"testing"

	"github.com/digitalocean/godo"

	domock "github.com/hstern/fj-bellows/internal/provider/digitalocean/mock"
)

func TestClientRecordsConcurrentCalls(t *testing.T) {
	fake := &domock.Client{
		CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
			return godo.Droplet{}, nil
		},
		GetDropletFn:    func(context.Context, int) (godo.Droplet, error) { return godo.Droplet{}, nil },
		DeleteDropletFn: func(context.Context, int) error { return nil },
	}
	const count = 32
	var wg sync.WaitGroup
	for id := range count {
		wg.Go(func() {
			_, _ = fake.CreateDroplet(context.Background(), godo.DropletCreateRequest{
				Name: "worker", Tags: []string{"tag"},
			})
			_, _ = fake.GetDroplet(context.Background(), id+1)
			_ = fake.DeleteDroplet(context.Background(), id+1)
		})
	}
	wg.Wait()

	if got := len(fake.CreateCalls()); got != count {
		t.Fatalf("create calls = %d, want %d", got, count)
	}
	if got := len(fake.DeleteCalls()); got != count {
		t.Fatalf("delete calls = %d, want %d", got, count)
	}
	if got := len(fake.GetCalls()); got != count {
		t.Fatalf("get calls = %d, want %d", got, count)
	}

	createCalls := fake.CreateCalls()
	createCalls[0].Tags[0] = "mutated"
	if fake.CreateCalls()[0].Tags[0] != "tag" {
		t.Fatal("CreateCalls did not return a defensive copy")
	}
}
