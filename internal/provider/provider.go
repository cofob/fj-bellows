// Package provider defines the cloud-provider abstraction and an in-tree
// registry. Providers register themselves by name (typically in init) and the
// core selects one by config. The core hands each provider the opaque
// provider_config yaml.Node to decode into its own struct.
package provider

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

// BillingModel determines the teardown policy the core applies to idle nodes.
type BillingModel int

const (
	// BillingPerSecond bills by the second (AWS/GCP/Azure now). Warm-holding is
	// pointless; the core uses a plain idle timeout.
	BillingPerSecond BillingModel = iota

	// BillingHourlyRoundUp bills whole hours rounded up (Linode, Hetzner, old
	// AWS). The core keeps nodes warm for the paid hour and kills idle nodes
	// just before each hour boundary (the :55 rule).
	BillingHourlyRoundUp
)

func (b BillingModel) String() string {
	switch b {
	case BillingPerSecond:
		return "per-second"
	case BillingHourlyRoundUp:
		return "hourly-round-up"
	default:
		return "unknown"
	}
}

// Spec describes a VM to provision. It is provider-agnostic; the cloud-init
// UserData is rendered by the core and is identical across providers.
type Spec struct {
	Tag           string   // stamped for reconcile + orphan sweep
	Name          string   // unique instance label
	Role          string   // worker by default; builder resources stay out of List
	InstanceType  string   // provider machine/size slug selected by the tier
	ImageID       string   // optional provider image/snapshot override
	UserData      string   // rendered cloud-init (plain text; provider encodes as needed)
	AuthorizedKey string   // SSH public key (authorized_keys line) for the orchestrator
	Labels        []string // Forgejo runner labels this VM will serve
}

// ResetSpec describes an in-place rebuild of an existing worker. Providers
// that can return a server to a clean managed image implement Resetter. The
// server allocation (and therefore its billing anchor) remains the same.
type ResetSpec struct {
	ImageID       string
	UserData      string
	AuthorizedKey string
}

// Resetter is an optional provider capability used by one-job tiers that can
// rebuild an existing allocation instead of deleting it.
type Resetter interface {
	Reset(ctx context.Context, id string, spec ResetSpec) (Instance, error)
}

// ManagedImage is the provider-neutral view of an owned golden image.
type ManagedImage struct {
	ID          string
	Name        string
	Fingerprint string
	CreatedAt   time.Time
	SizeBytes   int64
}

// ImageSpec describes a snapshot captured from a fully prepared builder.
type ImageSpec struct {
	Tag              string
	Name             string
	SourceInstanceID string
	Fingerprint      string
}

// ManagedImageProvider is an optional provider capability used to build,
// discover, rotate, and remove daemon-owned golden images.
type ManagedImageProvider interface {
	CreateImage(ctx context.Context, spec ImageSpec) (ManagedImage, error)
	DeleteImage(ctx context.Context, id string) error
	ListImages(ctx context.Context, tag string) ([]ManagedImage, error)
}

// BuilderProvider exposes daemon-owned image-builder VMs separately from
// normal workers. Builders are intentionally absent from Provider.List so
// they cannot be dispatched, but the core still needs cloud truth to reap a
// builder left behind by a process crash.
type BuilderProvider interface {
	ListBuilders(ctx context.Context, tag string) ([]Instance, error)
}

// BuilderPromoter atomically changes an owned image-builder allocation into
// a normal worker allocation. Snapshot-reset tiers use it after capturing the
// first golden image so the already-paid builder VM can be rebuilt in place
// and serve the waiting job instead of being deleted and replaced by a second
// allocation. The durable generation is marked unclean before promotion, so a
// crash in the hand-off can only cause the promoted VM to be drained. The
// provider must verify id belongs to the exact supplied ownership tag before
// changing its role.
type BuilderPromoter interface {
	PromoteBuilder(ctx context.Context, id, tag string) error
}

// PriceQuote is an immutable list-price observation. Monetary values are
// integer nanounits of Currency so accounting never depends on float64.
type PriceQuote struct {
	InstanceType         string
	Currency             string
	PerHourNanos         int64
	PerMonthNanos        int64
	BillingQuantum       time.Duration
	MinimumDuration      time.Duration
	MinimumChargeNanos   int64
	SnapshotGBMonthNanos int64
	Source               string
	ObservedAt           time.Time
}

// Pricer is an optional provider capability. A missing quote never prevents
// provisioning; callers record the resource as unpriced instead.
type Pricer interface {
	Quote(ctx context.Context, instanceType string) (PriceQuote, error)
}

// Instance is the provider's view of a running VM. CreatedAt comes from the
// provider's own clock and anchors the billing-hour timer, so the core can
// rebuild teardown timers purely from List after a restart.
type Instance struct {
	ID   string
	Name string
	// IPv4 is the public IPv4 (the legacy dial address under transport.mode=ssh).
	// Providers that dispatch by container exec (docker) leave it empty.
	IPv4 string
	// VPCIPv4 is the VPC-side IPv4 when the provider has VPC attachment
	// configured; empty otherwise. Under transport.mode=cache-gateway
	// (FJB-54) this is the dial address the orchestrator uses. Population
	// is provider-specific and may be lazy — providers that can't resolve
	// it cheaply on List may leave it empty until first needed.
	VPCIPv4   string
	CreatedAt time.Time
	Tag       string
}

// InfoProvider is the optional surface providers implement to expose
// operator-debug info via the control plane's ProviderInfo RPC. Providers
// don't have to implement this; the control plane's adapter type-asserts
// and emits an empty info map for providers that don't.
//
// Keys are stable, provider-documented slugs (per provider's README).
// Values are operator-readable strings — don't include secrets.
type InfoProvider interface {
	Info(ctx context.Context) map[string]string
}

// Provider is the in-tree cloud abstraction.
type Provider interface {
	// Configure decodes the opaque provider_config node into the provider's
	// own struct and prepares any client/credentials. ctx bounds any
	// network calls the provider makes during startup (e.g. resolving
	// firewall sentinels). tag is the orchestrator's cfg.Tag, passed in
	// here so the provider can stand up any tag-scoped resources at
	// startup rather than deferring them to the first Provision call.
	Configure(ctx context.Context, tag string, node yaml.Node) error

	// Provision creates a VM and returns it once the provider reports it as
	// created (not necessarily booted; the core waits for SSH readiness).
	Provision(ctx context.Context, spec Spec) (Instance, error)

	// Destroy deletes the VM with the given provider ID.
	Destroy(ctx context.Context, id string) error

	// List returns all instances carrying the given tag. It powers reconcile
	// and the orphan sweep, so it must reflect the provider's ground truth.
	List(ctx context.Context, tag string) ([]Instance, error)

	// BillingModel reports how this provider bills, which selects the teardown
	// policy.
	BillingModel() BillingModel
}

var registry = map[string]func() Provider{}

// Register adds a provider constructor under name. Call from init.
func Register(name string, f func() Provider) {
	registry[name] = f
}

// New constructs the provider registered under name.
func New(name string) (Provider, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q (registered: %v)", name, Names())
	}
	return f(), nil
}

// Names lists registered provider names, sorted.
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
