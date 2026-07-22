package orchestrator

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	omock "github.com/hstern/fj-bellows/internal/orchestrator/mock"
	"github.com/hstern/fj-bellows/internal/provider"
	pmock "github.com/hstern/fj-bellows/internal/provider/mock"
)

type capacityImageProvider struct {
	*pmock.Provider

	createStarted chan struct{}
	createRelease chan struct{}
	startOnce     sync.Once
}

func (p *capacityImageProvider) CreateImage(ctx context.Context, spec provider.ImageSpec) (provider.ManagedImage, error) {
	p.startOnce.Do(func() { close(p.createStarted) })
	select {
	case <-p.createRelease:
		return provider.ManagedImage{
			ID: "golden-image", Name: spec.Name, Fingerprint: spec.Fingerprint, CreatedAt: time.Now(),
		}, nil
	case <-ctx.Done():
		return provider.ManagedImage{}, ctx.Err()
	}
}

func (*capacityImageProvider) DeleteImage(context.Context, string) error { return nil }

func (*capacityImageProvider) ListImages(context.Context, string) ([]provider.ManagedImage, error) {
	return nil, nil
}

type capacityImageDispatcher struct{ omock.Dispatcher }

func (*capacityImageDispatcher) PrepareImage(context.Context, string, string) error { return nil }

type promotableImageProvider struct {
	*pmock.Provider

	mu       sync.Mutex
	promoted int
	resets   int
}

func (*promotableImageProvider) CreateImage(_ context.Context, spec provider.ImageSpec) (provider.ManagedImage, error) {
	return provider.ManagedImage{
		ID: "golden-image", Name: spec.Name, Fingerprint: spec.Fingerprint, CreatedAt: time.Now(),
	}, nil
}

func (*promotableImageProvider) DeleteImage(context.Context, string) error { return nil }

func (*promotableImageProvider) ListImages(context.Context, string) ([]provider.ManagedImage, error) {
	return nil, nil
}

func (p *promotableImageProvider) PromoteBuilder(context.Context, string, string) error {
	p.mu.Lock()
	p.promoted++
	p.mu.Unlock()
	return nil
}

func (p *promotableImageProvider) Reset(_ context.Context, id string, _ provider.ResetSpec) (provider.Instance, error) {
	p.mu.Lock()
	p.resets++
	p.mu.Unlock()
	return provider.Instance{ID: id, IPv4: fleetShortIP, CreatedAt: time.Now()}, nil
}

func (p *promotableImageProvider) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.promoted, p.resets
}

func TestSnapshotBuilderReservesCapacityBeforeMaxWarmPool(t *testing.T) {
	var rolesMu sync.Mutex
	var roles []string
	var nextID int
	baseProvider := &pmock.Provider{
		ProvisionFn: func(_ context.Context, spec provider.Spec) (provider.Instance, error) {
			rolesMu.Lock()
			roles = append(roles, spec.Role)
			nextID++
			n := nextID
			rolesMu.Unlock()
			return provider.Instance{
				ID: "vm-" + strconv.Itoa(n), IPv4: "192.0.2." + strconv.Itoa(n), CreatedAt: time.Now(),
			}, nil
		},
	}
	prov := &capacityImageProvider{
		Provider:      baseProvider,
		createStarted: make(chan struct{}),
		createRelease: make(chan struct{}),
	}
	cfg := baseConfig()
	cfg.Tier = fleetLongTier
	cfg.ProviderName = fleetHetznerName
	cfg.Driver = fleetHetznerDriver
	cfg.InstanceType = "cx33"
	cfg.ResetMode = resetModeSnapshot
	cfg.MaxScale = 3
	cfg.WarmInstances = cfg.MaxScale
	o := New(cfg, prov, &omock.JobSource{}, &capacityImageDispatcher{}, nil)

	result := o.Reconcile(t.Context())
	if result.Provisioned != 2 {
		t.Fatalf("worker provisions = %d, want 2 (third slot is the image builder)", result.Provisioned)
	}
	waitFor(t, "one builder and two workers provisioned", func() bool {
		return baseProvider.ProvisionCount() == cfg.MaxScale
	})
	select {
	case <-prov.createStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot builder did not reach image creation")
	}

	rolesMu.Lock()
	counts := map[string]int{}
	for _, role := range roles {
		counts[role]++
	}
	rolesMu.Unlock()
	if counts["builder"] != 1 || counts["worker"] != 2 {
		t.Fatalf("provision roles = %v, want builder:1 worker:2", counts)
	}
	if got := o.pool.Len() + o.pendingCount() + o.builderCount(); got != cfg.MaxScale {
		t.Fatalf("capacity in use = %d, want max scale %d", got, cfg.MaxScale)
	}

	close(prov.createRelease)
	o.wg.Wait()
	if got := baseProvider.DestroyCount(); got != 1 {
		t.Fatalf("builder DestroyCount = %d, want 1", got)
	}
}

func TestSnapshotBuilderBecomesFirstWarmWorkerWithoutSecondAllocation(t *testing.T) {
	baseProvider := &pmock.Provider{
		ProvisionFn: func(_ context.Context, spec provider.Spec) (provider.Instance, error) {
			if spec.Role != "builder" {
				t.Errorf("Provision role = %q, want builder", spec.Role)
			}
			return provider.Instance{
				ID: "builder-1", IPv4: fleetShortIP, CreatedAt: time.Now().Add(-time.Minute),
			}, nil
		},
	}
	prov := &promotableImageProvider{Provider: baseProvider}
	cfg := baseConfig()
	cfg.Tier = fleetLongTier
	cfg.ProviderName = fleetHetznerName
	cfg.Driver = fleetHetznerDriver
	cfg.InstanceType = "cx33"
	cfg.ResetMode = resetModeSnapshot
	cfg.MaxScale = 1
	cfg.WarmInstances = 1
	o := New(cfg, prov, &omock.JobSource{}, &capacityImageDispatcher{}, nil)

	result := o.Reconcile(t.Context())
	if result.Provisioned != 0 {
		t.Fatalf("parallel worker provisions = %d, want 0 while builder owns the only slot", result.Provisioned)
	}
	o.wg.Wait()

	promoted, resets := prov.counts()
	if promoted != 1 || resets != 1 {
		t.Fatalf("builder hand-off calls = promote:%d reset:%d, want 1 each", promoted, resets)
	}
	if baseProvider.ProvisionCount() != 1 || baseProvider.DestroyCount() != 0 {
		t.Fatalf("allocation calls = provision:%d destroy:%d, want 1 and 0",
			baseProvider.ProvisionCount(), baseProvider.DestroyCount())
	}
	nodes := o.pool.ByState(StateIdle)
	if len(nodes) != 1 || nodes[0].InstanceID != "builder-1" {
		t.Fatalf("idle pool after hand-off = %+v, want promoted builder", nodes)
	}
}

var (
	_ provider.ManagedImageProvider = (*capacityImageProvider)(nil)
	_ provider.ManagedImageProvider = (*promotableImageProvider)(nil)
	_ provider.BuilderPromoter      = (*promotableImageProvider)(nil)
	_ provider.Resetter             = (*promotableImageProvider)(nil)
	_ Dispatcher                    = (*capacityImageDispatcher)(nil)
	_ ImagePreparer                 = (*capacityImageDispatcher)(nil)
)
