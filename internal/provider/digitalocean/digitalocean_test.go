package digitalocean_test

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/digitalocean/godo"
	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/provider/digitalocean"
	domock "github.com/hstern/fj-bellows/internal/provider/digitalocean/mock"
)

const (
	validConfig = `
token: test-token
region: test-region
image: debian-13-x64
`
	workerTag         = "worker-tag"
	workerName        = "worker-42"
	genericWorkerName = "worker"
	testTag           = "tag"
	testSizeSlug      = "s-4vcpu-8gb"
	genericSize       = "size"
	publicTestIP      = "203.0.113.42"
)

func configNode(t *testing.T, value string) yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(value), &node); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return node
}

func configuredProvider(t *testing.T, cfg string, client digitalocean.Client) *digitalocean.DigitalOcean {
	t.Helper()
	d := digitalocean.NewWithClient(client)
	if err := d.Configure(context.Background(), "deployment-tag", configNode(t, cfg)); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	return d
}

func testDroplet() godo.Droplet {
	return godo.Droplet{
		ID:      42,
		Name:    workerName,
		Created: "2026-07-22T12:34:56Z",
		Status:  "active",
		Networks: &godo.Networks{V4: []godo.NetworkV4{
			{IPAddress: publicTestIP, Type: "public"},
			{IPAddress: "10.0.0.42", Type: "private"},
		}},
	}
}

func TestRegisteredInProviderRegistry(t *testing.T) {
	got, err := provider.New("digitalocean")
	if err != nil {
		t.Fatalf("provider.New: %v", err)
	}
	if _, ok := got.(*digitalocean.DigitalOcean); !ok {
		t.Fatalf("provider.New returned %T", got)
	}
}

func TestConfigureValidatesRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		cfg  string
		want string
	}{
		{name: "token", cfg: "region: test-region\nimage: image", want: "token"},
		{name: "region", cfg: "token: token\nimage: image", want: "region"},
		{name: "image", cfg: "token: token\nregion: test-region", want: "image"},
		{name: "bad key", cfg: validConfig + "ssh_key_ids: [0]", want: "positive IDs"},
		{name: "duplicate key", cfg: validConfig + "ssh_key_ids: [7, 7]", want: "duplicate"},
		{name: "unknown field", cfg: validConfig + "regoin: typo\n", want: "regoin"},
		{
			name: "unknown nested pricing field",
			cfg:  validConfig + "pricing_override:\n  instances:\n    s-1vcpu-1gb:\n      per_our: '0.01'\n",
			want: "per_our",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := digitalocean.NewWithClient(&domock.Client{})
			err := d.Configure(context.Background(), testTag, configNode(t, tt.cfg))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Configure error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestConfigureInfoDoesNotExposeToken(t *testing.T) {
	d := configuredProvider(t, validConfig+`
vpc_uuid: test-vpc
firewall_id: test-firewall
ssh_key_ids: [11, 12]
`, &domock.Client{})
	info := d.Info(context.Background())
	if info["region"] != "test-region" || info["image"] != "debian-13-x64" {
		t.Fatalf("Info = %#v", info)
	}
	if info["vpc_uuid"] != "test-vpc" || info["firewall_id"] != "test-firewall" {
		t.Fatalf("Info network fields = %#v", info)
	}
	if info["ssh_key_count"] != "2" || info[testTag] != "deployment-tag" {
		t.Fatalf("Info counts/tag = %#v", info)
	}
	if _, exists := info["token"]; exists {
		t.Fatal("Info exposed token")
	}
}

func TestProvisionCreatesTaggedPublicDroplet(t *testing.T) {
	fake := &domock.Client{
		CreateDropletFn: func(_ context.Context, _ godo.DropletCreateRequest) (godo.Droplet, error) {
			return testDroplet(), nil
		},
		AddDropletToFirewallFn: func(context.Context, string, int) error { return nil },
	}
	d := configuredProvider(t, validConfig+`
vpc_uuid: test-vpc
firewall_id: test-firewall
ssh_key_ids: [11, 12]
`, fake)
	inst, err := d.Provision(context.Background(), provider.Spec{
		Tag:          workerTag,
		Name:         workerName,
		InstanceType: testSizeSlug,
		UserData:     "#cloud-config\nusers: []\n",
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	calls := fake.CreateCalls()
	if len(calls) != 1 {
		t.Fatalf("create calls = %d", len(calls))
	}
	publicNetworking := true
	wantRequest := godo.DropletCreateRequest{
		Name:             workerName,
		Region:           "test-region",
		Size:             testSizeSlug,
		Image:            godo.DropletCreateImage{Slug: "debian-13-x64"},
		SSHKeys:          []godo.DropletCreateSSHKey{{ID: 11}, {ID: 12}},
		UserData:         "#cloud-config\nusers: []\n",
		Tags:             []string{workerTag},
		VPCUUID:          "test-vpc",
		PublicNetworking: &publicNetworking,
	}
	if !reflect.DeepEqual(calls[0], wantRequest) {
		t.Fatalf("create request = %#v, want %#v", calls[0], wantRequest)
	}
	wantFirewallCalls := []domock.FirewallCall{{FirewallID: "test-firewall", DropletID: 42}}
	if got := fake.FirewallCalls(); !reflect.DeepEqual(got, wantFirewallCalls) {
		t.Fatalf("firewall calls = %#v", got)
	}
	wantInstance := provider.Instance{
		ID:        "42",
		Name:      workerName,
		IPv4:      publicTestIP,
		VPCIPv4:   "10.0.0.42",
		CreatedAt: time.Date(2026, 7, 22, 12, 34, 56, 0, time.UTC),
		Tag:       workerTag,
	}
	if inst != wantInstance {
		t.Fatalf("instance = %#v, want %#v", inst, wantInstance)
	}
}

func TestProvisionUsesNumericImageOverride(t *testing.T) {
	fake := &domock.Client{CreateDropletFn: func(_ context.Context, _ godo.DropletCreateRequest) (godo.Droplet, error) {
		return testDroplet(), nil
	}}
	d := configuredProvider(t, validConfig, fake)
	_, err := d.Provision(context.Background(), provider.Spec{
		Tag: testTag, Name: genericWorkerName, InstanceType: genericSize, ImageID: "12345",
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	image := fake.CreateCalls()[0].Image
	if image.ID != 12345 || image.Slug != "" {
		t.Fatalf("image = %#v", image)
	}
}

func TestProvisionInstallsAuthorizedKeyWithoutAccountKeyIDs(t *testing.T) {
	fake := &domock.Client{
		CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
			return testDroplet(), nil
		},
	}
	d := configuredProvider(t, validConfig, fake)
	const authorizedKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest orchestrator@example"
	_, err := d.Provision(t.Context(), provider.Spec{
		Tag: testTag, Name: genericWorkerName, InstanceType: genericSize,
		UserData: "#cloud-config\nruncmd: []\n", AuthorizedKey: authorizedKey,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	request := fake.CreateCalls()[0]
	if len(request.SSHKeys) != 0 {
		t.Fatalf("SSHKeys = %#v, want none", request.SSHKeys)
	}
	if !strings.HasPrefix(request.UserData, "MIME-Version: 1.0") {
		t.Fatalf("user-data is not multipart MIME:\n%s", request.UserData)
	}
	if !strings.Contains(request.UserData, "#cloud-config\nruncmd: []") {
		t.Fatalf("user-data lost original cloud-config:\n%s", request.UserData)
	}
	if !strings.Contains(request.UserData, base64.StdEncoding.EncodeToString([]byte(authorizedKey))) {
		t.Fatalf("user-data does not contain encoded authorized key:\n%s", request.UserData)
	}
	if !strings.Contains(request.UserData, "grep -Fqx") {
		t.Fatalf("authorized-key installation is not idempotent:\n%s", request.UserData)
	}
}

func TestProvisionRejectsUserDataThatKeyInjectionMakesTooLarge(t *testing.T) {
	fake := &domock.Client{}
	d := configuredProvider(t, validConfig, fake)
	_, err := d.Provision(t.Context(), provider.Spec{
		Tag: testTag, Name: genericWorkerName, InstanceType: genericSize,
		UserData: strings.Repeat("x", 64*1024-1), AuthorizedKey: "ssh-ed25519 test-key",
	})
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("Provision error = %v, want final user-data size failure", err)
	}
	if len(fake.CreateCalls()) != 0 {
		t.Fatalf("create calls = %d, want 0", len(fake.CreateCalls()))
	}
}

func TestProvisionPollsUntilActiveWithPublicIPv4(t *testing.T) {
	creating := testDroplet()
	creating.Networks = nil
	fake := &domock.Client{
		CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
			return creating, nil
		},
		GetDropletFn: func(_ context.Context, id int) (godo.Droplet, error) {
			if id != 42 {
				t.Fatalf("GetDroplet id = %d, want 42", id)
			}
			return testDroplet(), nil
		},
	}
	d := configuredProvider(t, validConfig, fake)
	inst, err := d.Provision(t.Context(), provider.Spec{
		Tag: testTag, Name: genericWorkerName, InstanceType: genericSize,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if inst.IPv4 != publicTestIP {
		t.Fatalf("IPv4 = %q, want refreshed public address", inst.IPv4)
	}
	if got := fake.GetCalls(); !reflect.DeepEqual(got, []int{42}) {
		t.Fatalf("GetDroplet calls = %v, want [42]", got)
	}
}

func TestProvisionReadinessTimeoutCleansUpDroplet(t *testing.T) {
	creating := testDroplet()
	creating.Status = "new"
	creating.Networks = nil
	fake := &domock.Client{
		CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
			return creating, nil
		},
		GetDropletFn:    func(context.Context, int) (godo.Droplet, error) { return creating, nil },
		DeleteDropletFn: func(context.Context, int) error { return nil },
	}
	d := configuredProvider(t, validConfig, fake)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := d.Provision(ctx, provider.Spec{
		Tag: testTag, Name: genericWorkerName, InstanceType: genericSize,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Provision error = %v, want deadline exceeded", err)
	}
	if got := fake.DeleteCalls(); !reflect.DeepEqual(got, []int{42}) {
		t.Fatalf("cleanup calls = %v, want [42]", got)
	}
}

func TestProvisionReadinessGetFailureCleansUpDroplet(t *testing.T) {
	creating := testDroplet()
	creating.Networks = nil
	wantErr := errors.New("droplet API unavailable")
	fake := &domock.Client{
		CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
			return creating, nil
		},
		GetDropletFn:    func(context.Context, int) (godo.Droplet, error) { return godo.Droplet{}, wantErr },
		DeleteDropletFn: func(context.Context, int) error { return nil },
	}
	d := configuredProvider(t, validConfig, fake)
	_, err := d.Provision(t.Context(), provider.Spec{
		Tag: testTag, Name: genericWorkerName, InstanceType: genericSize,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Provision error = %v, want GetDroplet failure", err)
	}
	if got := fake.DeleteCalls(); !reflect.DeepEqual(got, []int{42}) {
		t.Fatalf("cleanup calls = %v, want [42]", got)
	}
}

func TestProvisionRejectsInvalidSpecBeforeCreate(t *testing.T) {
	fake := &domock.Client{}
	d := configuredProvider(t, validConfig, fake)
	tests := []provider.Spec{
		{Name: genericWorkerName, InstanceType: genericSize},
		{Tag: testTag, InstanceType: genericSize},
		{Tag: testTag, Name: genericWorkerName},
		{Tag: testTag, Name: genericWorkerName, InstanceType: genericSize, UserData: strings.Repeat("x", 64*1024+1)},
	}
	for _, spec := range tests {
		if _, err := d.Provision(context.Background(), spec); err == nil {
			t.Fatalf("Provision(%#v) unexpectedly succeeded", spec)
		}
	}
	if len(fake.CreateCalls()) != 0 {
		t.Fatalf("create calls = %d, want 0", len(fake.CreateCalls()))
	}
}

func TestProvisionCleansUpAfterFirewallFailure(t *testing.T) {
	attachErr := errors.New("attach failed")
	deleteErr := errors.New("delete failed")
	fake := &domock.Client{
		CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
			return testDroplet(), nil
		},
		AddDropletToFirewallFn: func(context.Context, string, int) error { return attachErr },
		DeleteDropletFn:        func(context.Context, int) error { return deleteErr },
	}
	d := configuredProvider(t, validConfig+"firewall_id: test-firewall\n", fake)
	_, err := d.Provision(context.Background(), provider.Spec{Tag: testTag, Name: genericWorkerName, InstanceType: genericSize})
	if !errors.Is(err, attachErr) || !errors.Is(err, deleteErr) {
		t.Fatalf("Provision error = %v, want both failures", err)
	}
	if got := fake.DeleteCalls(); len(got) != 1 || got[0] != 42 {
		t.Fatalf("delete calls = %v", got)
	}
}

func TestProvisionCreateFailureDoesNotDelete(t *testing.T) {
	wantErr := errors.New("create failed")
	fake := &domock.Client{CreateDropletFn: func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error) {
		return godo.Droplet{}, wantErr
	}}
	d := configuredProvider(t, validConfig, fake)
	_, err := d.Provision(context.Background(), provider.Spec{Tag: testTag, Name: genericWorkerName, InstanceType: genericSize})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Provision error = %v", err)
	}
	if len(fake.DeleteCalls()) != 0 {
		t.Fatalf("delete calls = %v, want none", fake.DeleteCalls())
	}
}

func TestDestroyValidatesAndDeletesID(t *testing.T) {
	fake := &domock.Client{DeleteDropletFn: func(context.Context, int) error { return nil }}
	d := configuredProvider(t, validConfig, fake)
	if err := d.Destroy(context.Background(), "not-an-id"); err == nil {
		t.Fatal("Destroy accepted a non-numeric ID")
	}
	if err := d.Destroy(context.Background(), "0"); err == nil {
		t.Fatal("Destroy accepted a zero ID")
	}
	if err := d.Destroy(context.Background(), "42"); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if got := fake.DeleteCalls(); len(got) != 1 || got[0] != 42 {
		t.Fatalf("delete calls = %v", got)
	}
}

func TestListReturnsProviderInstancesForOwnershipTag(t *testing.T) {
	fake := &domock.Client{ListDropletsByTagFn: func(_ context.Context, tag string) ([]godo.Droplet, error) {
		if tag != workerTag {
			t.Fatalf("tag = %q", tag)
		}
		return []godo.Droplet{testDroplet()}, nil
	}}
	d := configuredProvider(t, validConfig+"vpc_uuid: test-vpc\n", fake)
	instances, err := d.List(context.Background(), workerTag)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(instances) != 1 || instances[0].ID != "42" || instances[0].Tag != workerTag {
		t.Fatalf("instances = %#v", instances)
	}
	if instances[0].IPv4 != publicTestIP || instances[0].VPCIPv4 != "10.0.0.42" {
		t.Fatalf("instance addresses = %#v", instances[0])
	}
	if got := fake.ListCalls(); len(got) != 1 || got[0] != workerTag {
		t.Fatalf("list calls = %v", got)
	}
}

func TestListRejectsEmptyTagAndMalformedTimestamp(t *testing.T) {
	fake := &domock.Client{ListDropletsByTagFn: func(context.Context, string) ([]godo.Droplet, error) {
		droplet := testDroplet()
		droplet.Created = "not-a-time"
		return []godo.Droplet{droplet}, nil
	}}
	d := configuredProvider(t, validConfig, fake)
	if _, err := d.List(context.Background(), " "); err == nil {
		t.Fatal("List accepted an empty tag")
	}
	if _, err := d.List(context.Background(), testTag); err == nil || !strings.Contains(err.Error(), "created_at") {
		t.Fatalf("List error = %v", err)
	}
}

func TestBillingModelIsPerSecond(t *testing.T) {
	if got := (&digitalocean.DigitalOcean{}).BillingModel(); got != provider.BillingPerSecond {
		t.Fatalf("BillingModel = %v", got)
	}
}
