package hetzner_test

import (
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/provider/hetzner"
	hmock "github.com/hstern/fj-bellows/internal/provider/hetzner/mock"
)

const validConfig = `
token: test-token
location: test-location
image: debian-13
`

const (
	testInstanceType = "cx33"
	testTag          = "tag"
	testBuilder      = "builder"
	testWorker       = "worker"
	testTierTag      = "tier-tag"
)

var createdAt = time.Date(2026, 7, 22, 12, 34, 56, 0, time.UTC)

func configNode(t *testing.T, value string) yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(value), &node); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return node
}

func configuredProvider(t *testing.T, cfg string, client hetzner.Client) *hetzner.Hetzner {
	t.Helper()
	h := hetzner.NewWithClient(client)
	if err := h.Configure(context.Background(), "deployment-tag", configNode(t, cfg)); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	return h
}

func serverForRequest(id int64, req hetzner.CreateServerRequest) hetzner.Server {
	return hetzner.Server{
		ID: id, Name: req.Name, PublicIPv4: "203.0.113.42", PrivateIPv4: "10.0.0.42",
		CreatedAt: createdAt, Labels: req.Labels,
	}
}

func TestRegisteredInProviderRegistry(t *testing.T) {
	got, err := provider.New("hetzner")
	if err != nil {
		t.Fatalf("provider.New: %v", err)
	}
	if _, ok := got.(*hetzner.Hetzner); !ok {
		t.Fatalf("provider.New returned %T", got)
	}
}

func TestConfigureValidationAndInfo(t *testing.T) {
	tests := []struct {
		name string
		cfg  string
		want string
	}{
		{name: "token", cfg: "location: test\nimage: debian", want: "token"},
		{name: "location", cfg: "token: test\nimage: debian", want: "location"},
		{name: "image", cfg: "token: test\nlocation: fsn1", want: "image"},
		{name: "bad network", cfg: validConfig + "network_id: -1\n", want: "network_id"},
		{name: "bad firewall", cfg: validConfig + "firewall_ids: [0]\n", want: "positive IDs"},
		{name: "duplicate firewall", cfg: validConfig + "firewall_ids: [7, 7]\n", want: "duplicate"},
		{name: "unknown field", cfg: validConfig + "locaton: typo\n", want: "locaton"},
		{
			name: "unknown nested pricing field",
			cfg:  validConfig + "pricing_override:\n  instances:\n    cx33:\n      per_our: '0.01'\n",
			want: "per_our",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := hetzner.NewWithClient(&hmock.Client{})
			err := h.Configure(context.Background(), testTag, configNode(t, tt.cfg))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Configure error = %v, want substring %q", err, tt.want)
			}
		})
	}

	h := configuredProvider(t, validConfig+"network_id: 12\nfirewall_ids: [21, 22]\n", &hmock.Client{})
	info := h.Info(context.Background())
	if info["location"] != "test-location" || info["image"] != "debian-13" ||
		info["network_id"] != "12" || info["firewall_ids"] != "21,22" || info[testTag] != "deployment-tag" {
		t.Fatalf("Info = %#v", info)
	}
	if _, exists := info["token"]; exists {
		t.Fatal("Info exposed provider token")
	}
}

//nolint:gocyclo // The test checks every field of one provider create/response translation.
func TestProvisionCreatesPublicServerWithCloudInitKey(t *testing.T) {
	fake := &hmock.Client{CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
		return serverForRequest(42, req), nil
	}}
	h := configuredProvider(t, validConfig+"network_id: 12\nfirewall_ids: [21, 22]\n", fake)
	key := "ssh-ed25519 AAAAC3Nza-test orchestrator"
	inst, err := h.Provision(context.Background(), provider.Spec{
		Tag: "worker-tag", Name: "worker-42", InstanceType: testInstanceType,
		UserData: "#cloud-config\nruncmd: []\n", AuthorizedKey: key,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	calls := fake.CreateServerCalls()
	if len(calls) != 1 {
		t.Fatalf("create calls = %d", len(calls))
	}
	req := calls[0]
	if req.Name != "worker-42" || req.InstanceType != testInstanceType || req.Image != "debian-13" || req.Location != "test-location" {
		t.Fatalf("create identity = %#v", req)
	}
	if req.NetworkID != 12 || len(req.FirewallIDs) != 2 || req.FirewallIDs[1] != 22 {
		t.Fatalf("create attachments = %#v", req)
	}
	if !strings.HasPrefix(req.UserData, "MIME-Version: 1.0") ||
		!strings.Contains(req.UserData, base64.StdEncoding.EncodeToString([]byte(key))) {
		t.Fatalf("authorized key was not encoded into multipart cloud-init:\n%s", req.UserData)
	}
	if inst.ID != "42" || inst.IPv4 != "203.0.113.42" || inst.VPCIPv4 != "10.0.0.42" ||
		inst.Tag != "worker-tag" || !inst.CreatedAt.Equal(createdAt) {
		t.Fatalf("instance = %#v", inst)
	}
}

func TestProvisionValidatesAndCleansUpMalformedResult(t *testing.T) {
	fake := &hmock.Client{
		CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
			server := serverForRequest(42, req)
			server.PublicIPv4 = ""
			return server, nil
		},
		DeleteServerFn: func(context.Context, int64) error { return nil },
	}
	h := configuredProvider(t, validConfig, fake)
	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: testTag, Name: testWorker, InstanceType: testInstanceType,
	}); err == nil || !strings.Contains(err.Error(), "public IPv4") {
		t.Fatalf("Provision error = %v", err)
	}
	calls := fake.Calls()
	if len(calls) != 2 || calls[1].Method != "DeleteServer" || calls[1].ID != 42 {
		t.Fatalf("calls = %#v", calls)
	}

	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: testTag, Name: testWorker, InstanceType: testInstanceType, Role: "unknown",
	}); err == nil {
		t.Fatal("Provision accepted unknown role")
	}
	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: testTag, Name: testWorker, InstanceType: testInstanceType, UserData: strings.Repeat("x", 32*1024+1),
	}); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("oversize user-data error = %v", err)
	}
}

func TestListExcludesBuildersAndForeignResources(t *testing.T) {
	var worker, builder hetzner.Server
	fake := &hmock.Client{CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
		server := serverForRequest(int64(len(req.Name)+40), req)
		if req.Name == testWorker {
			worker = server
		} else {
			builder = server
		}
		return server, nil
	}}
	h := configuredProvider(t, validConfig, fake)
	for _, spec := range []provider.Spec{
		{Tag: testTag, Name: testWorker, InstanceType: testInstanceType},
		{Tag: testTag, Name: testBuilder, Role: testBuilder, InstanceType: testInstanceType},
	} {
		if _, err := h.Provision(context.Background(), spec); err != nil {
			t.Fatalf("Provision(%s): %v", spec.Name, err)
		}
	}
	foreign := worker
	foreign.ID = 99
	foreign.Labels = map[string]string{"fjb-managed": "true"}
	fake.ListServersFn = func(context.Context, string) ([]hetzner.Server, error) {
		return []hetzner.Server{worker, builder, foreign}, nil
	}
	got, err := h.List(context.Background(), testTag)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].Name != testWorker {
		t.Fatalf("List = %#v", got)
	}
	builders, err := h.ListBuilders(context.Background(), testTag)
	if err != nil {
		t.Fatalf("ListBuilders: %v", err)
	}
	if len(builders) != 1 || builders[0].Name != testBuilder {
		t.Fatalf("ListBuilders = %#v", builders)
	}
	calls := fake.Calls()
	if len(calls) < 2 || !strings.Contains(calls[len(calls)-2].Selector, "fjb-role=worker") ||
		!strings.Contains(calls[len(calls)-1].Selector, "fjb-role=builder") {
		t.Fatalf("worker/builder list calls = %#v", calls)
	}
}

func TestPromoteBuilderPreservesEveryLabelAndRejectsNonBuilders(t *testing.T) {
	var builder, foreign hetzner.Server
	fake := &hmock.Client{
		CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
			if req.Name == "foreign-builder" {
				foreign = serverForRequest(45, req)
				return foreign, nil
			}
			builder = serverForRequest(43, req)
			return builder, nil
		},
	}
	h := configuredProvider(t, validConfig, fake)
	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: testTierTag, Name: testBuilder, Role: testBuilder, InstanceType: testInstanceType,
	}); err != nil {
		t.Fatalf("Provision builder: %v", err)
	}
	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: "foreign-tier-tag", Name: "foreign-builder", Role: testBuilder, InstanceType: testInstanceType,
	}); err != nil {
		t.Fatalf("Provision foreign builder: %v", err)
	}
	// Promotion must preserve metadata it does not currently interpret, such
	// as fingerprint chunks added by a later image contract.
	builder.Labels["fjb-fingerprint-parts"] = "1"
	builder.Labels["fjb-fingerprint-0"] = "future-fingerprint-chunk"
	worker := builder
	worker.ID = 44
	worker.Labels = maps.Clone(builder.Labels)
	worker.Labels["fjb-role"] = "worker"
	fake.GetServerFn = func(_ context.Context, id int64) (hetzner.Server, error) {
		switch id {
		case builder.ID:
			return builder, nil
		case worker.ID:
			return worker, nil
		default:
			return foreign, nil
		}
	}
	fake.UpdateServerLabelsFn = func(_ context.Context, id int64, labels map[string]string) (hetzner.Server, error) {
		after := builder
		after.ID = id
		after.Labels = maps.Clone(labels)
		return after, nil
	}

	expected := maps.Clone(builder.Labels)
	expected["fjb-role"] = "worker"
	if err := h.PromoteBuilder(context.Background(), "43", testTierTag); err != nil {
		t.Fatalf("PromoteBuilder: %v", err)
	}
	updates := fake.UpdateServerLabelsCalls()
	if len(updates) != 1 || updates[0].ServerID != 43 || !maps.Equal(updates[0].Labels, expected) {
		t.Fatalf("label updates = %#v, want only role changed to worker in %#v", updates, expected)
	}
	if builder.Labels["fjb-role"] != "builder" {
		t.Fatalf("PromoteBuilder mutated source labels: %#v", builder.Labels)
	}

	for name, id := range map[string]string{"worker": "44", "foreign": "45"} {
		t.Run(name, func(t *testing.T) {
			err := h.PromoteBuilder(context.Background(), id, testTierTag)
			if err == nil || !strings.Contains(err.Error(), "not an owned builder") {
				t.Fatalf("PromoteBuilder(%s) error = %v", id, err)
			}
		})
	}
	if updates = fake.UpdateServerLabelsCalls(); len(updates) != 1 {
		t.Fatalf("rejected servers caused label updates: %#v", updates)
	}
}

func TestManagedSnapshotPoweroffOrderingAndFingerprintRoundTrip(t *testing.T) {
	var builder hetzner.Server
	fake := &hmock.Client{
		CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
			builder = serverForRequest(43, req)
			return builder, nil
		},
		GetServerFn:      func(context.Context, int64) (hetzner.Server, error) { return builder, nil },
		PowerOffServerFn: func(context.Context, int64) error { return nil },
		CreateSnapshotFn: func(_ context.Context, req hetzner.CreateSnapshotRequest) (hetzner.Image, error) {
			return hetzner.Image{
				ID: 700, Name: req.Name, CreatedAt: createdAt.Add(time.Minute), SizeBytes: 8_000_000_000,
				Labels: req.Labels,
			}, nil
		},
	}
	h := configuredProvider(t, validConfig, fake)
	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: testTierTag, Name: testBuilder, Role: testBuilder, InstanceType: testInstanceType,
	}); err != nil {
		t.Fatalf("Provision builder: %v", err)
	}
	fingerprint := strings.Repeat("abcdef0123456789", 4)
	image, err := h.CreateImage(context.Background(), provider.ImageSpec{
		Tag: testTierTag, Name: "golden-image", SourceInstanceID: "43", Fingerprint: fingerprint,
	})
	if err != nil {
		t.Fatalf("CreateImage: %v", err)
	}
	if image.ID != "700" || image.Name != "golden-image" || image.Fingerprint != fingerprint || image.SizeBytes != 8_000_000_000 {
		t.Fatalf("image = %#v", image)
	}
	calls := fake.Calls()
	methods := make([]string, 0, len(calls))
	for _, call := range calls {
		methods = append(methods, call.Method)
	}
	joined := strings.Join(methods, ",")
	if !strings.Contains(joined, "GetServer,PowerOffServer,CreateSnapshot") {
		t.Fatalf("call order = %s", joined)
	}
}

func TestResetRequiresManagedImageAndPreservesCreatedAt(t *testing.T) {
	var worker, builder hetzner.Server
	var managedImage hetzner.Image
	fake := &hmock.Client{
		CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
			if req.Name == testWorker {
				worker = serverForRequest(42, req)
				return worker, nil
			}
			builder = serverForRequest(43, req)
			return builder, nil
		},
		GetServerFn: func(_ context.Context, id int64) (hetzner.Server, error) {
			if id == 42 {
				return worker, nil
			}
			return builder, nil
		},
		PowerOffServerFn: func(context.Context, int64) error { return nil },
		CreateSnapshotFn: func(_ context.Context, req hetzner.CreateSnapshotRequest) (hetzner.Image, error) {
			managedImage = hetzner.Image{ID: 700, Name: req.Name, CreatedAt: createdAt, Labels: req.Labels}
			return managedImage, nil
		},
		GetImageFn: func(context.Context, int64) (hetzner.Image, error) { return managedImage, nil },
		RebuildServerFn: func(_ context.Context, _, _ int64, _ string) (hetzner.Server, error) {
			after := worker
			after.CreatedAt = createdAt.Add(10 * time.Minute)
			return after, nil
		},
	}
	h := configuredProvider(t, validConfig, fake)
	for _, spec := range []provider.Spec{
		{Tag: testTierTag, Name: testWorker, InstanceType: testInstanceType},
		{Tag: testTierTag, Name: testBuilder, Role: testBuilder, InstanceType: testInstanceType},
	} {
		if _, err := h.Provision(context.Background(), spec); err != nil {
			t.Fatalf("Provision(%s): %v", spec.Name, err)
		}
	}
	if _, err := h.CreateImage(context.Background(), provider.ImageSpec{
		Tag: testTierTag, Name: "golden", SourceInstanceID: "43", Fingerprint: "fingerprint",
	}); err != nil {
		t.Fatalf("CreateImage: %v", err)
	}
	key := "ssh-ed25519 reset-key"
	inst, err := h.Reset(context.Background(), "42", provider.ResetSpec{
		ImageID: "700", UserData: "#cloud-config\n", AuthorizedKey: key,
	})
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if !inst.CreatedAt.Equal(createdAt) {
		t.Fatalf("CreatedAt = %s, want original %s", inst.CreatedAt, createdAt)
	}
	rebuilds := fake.RebuildServerCalls()
	if len(rebuilds) != 1 || rebuilds[0].ServerID != 42 || rebuilds[0].ImageID != 700 ||
		!strings.Contains(rebuilds[0].UserData, base64.StdEncoding.EncodeToString([]byte(key))) {
		t.Fatalf("rebuild calls = %#v", rebuilds)
	}

	foreign := managedImage
	foreign.Labels = map[string]string{"fjb-managed": "true", "fjb-role": "image"}
	fake.GetImageFn = func(context.Context, int64) (hetzner.Image, error) { return foreign, nil }
	if _, err := h.Reset(context.Background(), "42", provider.ResetSpec{ImageID: "700"}); err == nil {
		t.Fatal("Reset accepted foreign snapshot")
	}
}

func TestListAndDeleteManagedImages(t *testing.T) {
	var builder hetzner.Server
	var image hetzner.Image
	fake := &hmock.Client{
		CreateServerFn: func(_ context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
			builder = serverForRequest(43, req)
			return builder, nil
		},
		GetServerFn:      func(context.Context, int64) (hetzner.Server, error) { return builder, nil },
		PowerOffServerFn: func(context.Context, int64) error { return nil },
		CreateSnapshotFn: func(_ context.Context, req hetzner.CreateSnapshotRequest) (hetzner.Image, error) {
			image = hetzner.Image{ID: 700, Name: req.Name, CreatedAt: createdAt, Labels: req.Labels}
			return image, nil
		},
		DeleteImageFn: func(context.Context, int64) error { return nil },
	}
	h := configuredProvider(t, validConfig, fake)
	if _, err := h.Provision(context.Background(), provider.Spec{
		Tag: testTierTag, Name: testBuilder, Role: testBuilder, InstanceType: testInstanceType,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.CreateImage(context.Background(), provider.ImageSpec{
		Tag: testTierTag, Name: "golden", SourceInstanceID: "43", Fingerprint: "fp",
	}); err != nil {
		t.Fatal(err)
	}
	fake.ListImagesFn = func(context.Context, string) ([]hetzner.Image, error) {
		return []hetzner.Image{image, {ID: 701, Labels: map[string]string{"fjb-managed": "true"}}}, nil
	}
	images, err := h.ListImages(context.Background(), testTierTag)
	if err != nil || len(images) != 1 || images[0].Fingerprint != "fp" {
		t.Fatalf("ListImages = %#v, %v", images, err)
	}
	if err := h.DeleteImage(context.Background(), "700"); err != nil {
		t.Fatalf("DeleteImage: %v", err)
	}
	if err := h.DeleteImage(context.Background(), "bad"); err == nil {
		t.Fatal("DeleteImage accepted bad ID")
	}
}

func TestDestroyAndBillingModel(t *testing.T) {
	fake := &hmock.Client{DeleteServerFn: func(context.Context, int64) error { return nil }}
	h := configuredProvider(t, validConfig, fake)
	if err := h.Destroy(context.Background(), "42"); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if err := h.Destroy(context.Background(), "bad"); err == nil {
		t.Fatal("Destroy accepted bad ID")
	}
	if h.BillingModel() != provider.BillingHourlyRoundUp {
		t.Fatalf("BillingModel = %v", h.BillingModel())
	}
}

//nolint:gocyclo // The test verifies the full fixed-point catalog and complete-override quote shapes.
func TestQuoteCatalogAndOverride(t *testing.T) {
	observed := time.Date(2026, 7, 22, 18, 0, 0, 0, time.UTC)
	fake := &hmock.Client{GetPricingFn: func(context.Context) (hetzner.Catalog, error) {
		return hetzner.Catalog{
			Currency: "EUR", SnapshotGBMonth: "0.0123000000",
			ServerTypes: []hetzner.ServerTypePrice{
				{InstanceType: testInstanceType, Location: "other", PerHour: "99", PerMonth: "99"},
				{InstanceType: testInstanceType, Location: "test-location", PerHour: "0.0105000000", PerMonth: "6.49"},
			},
		}, nil
	}}
	h := configuredProvider(t, validConfig, fake)
	quote, err := h.Quote(context.Background(), testInstanceType)
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if quote.Currency != "EUR" || quote.PerHourNanos != 10_500_000 || quote.PerMonthNanos != 6_490_000_000 ||
		quote.SnapshotGBMonthNanos != 12_300_000 || quote.BillingQuantum != time.Hour || quote.MinimumDuration != time.Hour {
		t.Fatalf("quote = %#v", quote)
	}

	overrideFake := &hmock.Client{}
	overrideProvider := configuredProvider(t, validConfig+`
pricing_override:
  currency: USD
  instances:
    cx33:
      per_hour: "0.02"
      per_month: "12.5"
  snapshot_gb_month: "0.03"
`, overrideFake)
	// Pin time through the provider's public output by checking only that it
	// is populated; the injectable clock remains an implementation detail.
	overrideQuote, err := overrideProvider.Quote(context.Background(), testInstanceType)
	if err != nil {
		t.Fatalf("override Quote: %v", err)
	}
	if overrideQuote.Currency != "USD" || overrideQuote.PerHourNanos != 20_000_000 ||
		overrideQuote.PerMonthNanos != 12_500_000_000 || overrideQuote.SnapshotGBMonthNanos != 30_000_000 ||
		overrideQuote.Source != "config:pricing_override" || overrideQuote.ObservedAt.IsZero() {
		t.Fatalf("override quote = %#v (reference time %s)", overrideQuote, observed)
	}
	if len(overrideFake.Calls()) != 0 {
		t.Fatalf("complete override unexpectedly called catalog: %#v", overrideFake.Calls())
	}
}

func TestMockIsConcurrencySafe(t *testing.T) {
	fake := &hmock.Client{DeleteServerFn: func(context.Context, int64) error { return nil }}
	const calls = 32
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			if err := fake.DeleteServer(context.Background(), int64(i+1)); err != nil {
				t.Errorf("DeleteServer: %v", err)
			}
		})
	}
	wg.Wait()
	if len(fake.Calls()) != calls {
		t.Fatalf("calls = %d", len(fake.Calls()))
	}
}

func TestErrorsAreWrapped(t *testing.T) {
	want := errors.New("cloud failed")
	fake := &hmock.Client{CreateServerFn: func(context.Context, hetzner.CreateServerRequest) (hetzner.Server, error) {
		return hetzner.Server{}, want
	}}
	h := configuredProvider(t, validConfig, fake)
	_, err := h.Provision(context.Background(), provider.Spec{Tag: testTag, Name: testWorker, InstanceType: testInstanceType})
	if !errors.Is(err, want) {
		t.Fatalf("Provision error = %v, want wraps %v", err, want)
	}
}
