// Package hetzner implements ephemeral Hetzner Cloud servers, managed golden
// snapshots, and in-place rebuilds for the provider abstraction.
package hetzner

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/provider"
)

const (
	maxUserDataBytes        = 32 * 1024
	provisionCleanupTimeout = 30 * time.Second
)

type config struct {
	Token           string           `yaml:"token"`
	Location        string           `yaml:"location"`
	Locations       []string         `yaml:"locations"`
	Image           string           `yaml:"image"`
	NetworkID       int64            `yaml:"network_id"`
	FirewallIDs     []int64          `yaml:"firewall_ids"`
	PricingOverride *pricingOverride `yaml:"pricing_override"`
}

func (c *config) normalizeAndValidate() error {
	c.Token = strings.TrimSpace(c.Token)
	c.Location = strings.TrimSpace(c.Location)
	for i := range c.Locations {
		c.Locations[i] = strings.TrimSpace(c.Locations[i])
	}
	c.Image = strings.TrimSpace(c.Image)
	if c.Location != "" && len(c.Locations) != 0 {
		return errors.New("hetzner: provider config: location and locations are mutually exclusive")
	}
	if c.Location != "" {
		c.Locations = []string{c.Location}
	} else if len(c.Locations) != 0 {
		seenLocations := make(map[string]struct{}, len(c.Locations))
		for _, location := range c.Locations {
			if location == "" {
				return errors.New("hetzner: provider config: locations must contain non-empty names")
			}
			if _, exists := seenLocations[location]; exists {
				return fmt.Errorf("hetzner: provider config: duplicate locations entry %q", location)
			}
			seenLocations[location] = struct{}{}
		}
		c.Location = c.Locations[0]
	}
	var missing []string
	if c.Token == "" {
		missing = append(missing, "token")
	}
	if len(c.Locations) == 0 {
		missing = append(missing, "location or locations")
	}
	if c.Image == "" {
		missing = append(missing, "image")
	}
	if len(missing) != 0 {
		return fmt.Errorf("hetzner: provider config missing: %s", strings.Join(missing, ", "))
	}
	if c.NetworkID < 0 {
		return fmt.Errorf("hetzner: provider config: network_id must be positive, got %d", c.NetworkID)
	}
	seenFirewalls := make(map[int64]struct{}, len(c.FirewallIDs))
	for _, id := range c.FirewallIDs {
		if id <= 0 {
			return fmt.Errorf("hetzner: provider config: firewall_ids must contain positive IDs, got %d", id)
		}
		if _, ok := seenFirewalls[id]; ok {
			return fmt.Errorf("hetzner: provider config: duplicate firewall_ids entry %d", id)
		}
		seenFirewalls[id] = struct{}{}
	}
	if c.PricingOverride != nil {
		if err := c.PricingOverride.normalizeAndValidate(); err != nil {
			return fmt.Errorf("hetzner: provider config: pricing_override: %w", err)
		}
	}
	return nil
}

// Hetzner provisions servers through the official hcloud-go v2 client. The
// SDK is kept behind Client so provider tests remain fast and hermetic.
type Hetzner struct {
	cfg    config
	client Client
	tag    string
	now    func() time.Time
}

var (
	_ provider.Provider             = (*Hetzner)(nil)
	_ provider.InfoProvider         = (*Hetzner)(nil)
	_ provider.Resetter             = (*Hetzner)(nil)
	_ provider.ManagedImageProvider = (*Hetzner)(nil)
	_ provider.BuilderProvider      = (*Hetzner)(nil)
	_ provider.BuilderPromoter      = (*Hetzner)(nil)
	_ provider.Pricer               = (*Hetzner)(nil)
)

func init() {
	provider.Register("hetzner", func() provider.Provider { return &Hetzner{} })
}

// NewWithClient constructs a provider around an injected API client. It is
// intended for tests and alternate transports; Configure still validates the
// complete provider configuration.
func NewWithClient(client Client) *Hetzner {
	return &Hetzner{client: client, now: time.Now}
}

// Configure decodes the opaque provider block and constructs the hcloud
// adapter. It performs no cloud mutation.
func (h *Hetzner) Configure(_ context.Context, tag string, node yaml.Node) error {
	var cfg config
	if err := provider.DecodeConfig(node, &cfg); err != nil {
		return fmt.Errorf("hetzner: decode provider config: %w", err)
	}
	if err := cfg.normalizeAndValidate(); err != nil {
		return err
	}
	if strings.TrimSpace(tag) == "" {
		return errors.New("hetzner: deployment tag must not be empty")
	}
	if _, err := ownershipLabels(tag, roleWorker); err != nil {
		return fmt.Errorf("hetzner: deployment tag: %w", err)
	}

	h.cfg = cfg
	h.tag = tag
	if h.client == nil {
		h.client = newHCloudClient(cfg.Token)
	}
	if h.now == nil {
		h.now = time.Now
	}
	return nil
}

// Provision creates a public-IPv4 server. Empty Role means a normal worker;
// builders receive distinct ownership labels and are intentionally excluded
// from List so reconcile cannot adopt them as ready workers.
func (h *Hetzner) Provision(ctx context.Context, spec provider.Spec) (provider.Instance, error) {
	if h.client == nil {
		return provider.Instance{}, errors.New("hetzner: provider is not configured")
	}
	role, err := validateProvisionSpec(spec)
	if err != nil {
		return provider.Instance{}, err
	}
	labels, err := ownershipLabels(spec.Tag, role)
	if err != nil {
		return provider.Instance{}, fmt.Errorf("hetzner: provision labels: %w", err)
	}

	imageRef := h.cfg.Image
	if strings.TrimSpace(spec.ImageID) != "" {
		image, imageErr := h.ownedImage(ctx, spec.ImageID)
		if imageErr != nil {
			return provider.Instance{}, imageErr
		}
		if !hasOwnership(image.Labels, spec.Tag, roleImage) {
			return provider.Instance{}, fmt.Errorf("hetzner: image %s is not a managed snapshot for tag %q", spec.ImageID, spec.Tag)
		}
		imageRef = strconv.FormatInt(image.ID, 10)
	}
	userData, err := renderUserData(spec.UserData, spec.AuthorizedKey)
	if err != nil {
		return provider.Instance{}, err
	}

	server, err := h.createServer(ctx, CreateServerRequest{
		Name:         spec.Name,
		InstanceType: spec.InstanceType,
		Image:        imageRef,
		UserData:     userData,
		Labels:       labels,
		NetworkID:    h.cfg.NetworkID,
		FirewallIDs:  append([]int64(nil), h.cfg.FirewallIDs...),
	})
	if err != nil {
		return provider.Instance{}, fmt.Errorf("hetzner: create server: %w", err)
	}
	inst, err := toInstance(server, spec.Tag, h.cfg.NetworkID != 0)
	if err != nil {
		return provider.Instance{}, h.cleanupFailedProvision(ctx, server.ID, err)
	}
	if inst.IPv4 == "" {
		return provider.Instance{}, h.cleanupFailedProvision(ctx, server.ID,
			fmt.Errorf("hetzner: created server %d has no public IPv4", server.ID))
	}
	return inst, nil
}

func (h *Hetzner) createServer(ctx context.Context, req CreateServerRequest) (Server, error) {
	locationErrors := make([]error, 0, len(h.cfg.Locations))
	for _, location := range h.cfg.Locations {
		req.Location = location
		server, err := h.client.CreateServer(ctx, req)
		if err == nil {
			return server, nil
		}
		locationErrors = append(locationErrors, fmt.Errorf("location %s: %w", location, err))
		if ctx.Err() != nil || !errors.Is(err, ErrLocationUnavailable) {
			return Server{}, errors.Join(locationErrors...)
		}
	}
	return Server{}, fmt.Errorf("all configured locations unavailable: %w", errors.Join(locationErrors...))
}

func validateProvisionSpec(spec provider.Spec) (string, error) {
	var missing []string
	if strings.TrimSpace(spec.Tag) == "" {
		missing = append(missing, "tag")
	}
	if strings.TrimSpace(spec.Name) == "" {
		missing = append(missing, "name")
	}
	if strings.TrimSpace(spec.InstanceType) == "" {
		missing = append(missing, "instance_type")
	}
	if len(missing) != 0 {
		return "", fmt.Errorf("hetzner: provision spec missing: %s", strings.Join(missing, ", "))
	}
	role := strings.TrimSpace(spec.Role)
	if role == "" {
		role = roleWorker
	}
	if role != roleWorker && role != roleBuilder {
		return "", fmt.Errorf("hetzner: unsupported provision role %q", spec.Role)
	}
	return role, nil
}

func renderUserData(userData, authorizedKey string) (string, error) {
	rendered, err := withAuthorizedKey(userData, authorizedKey)
	if err != nil {
		return "", fmt.Errorf("hetzner: render authorized key: %w", err)
	}
	if len([]byte(rendered)) > maxUserDataBytes {
		return "", fmt.Errorf("hetzner: user-data is %d bytes; maximum is %d", len([]byte(rendered)), maxUserDataBytes)
	}
	return rendered, nil
}

func (h *Hetzner) cleanupFailedProvision(ctx context.Context, id int64, cause error) error {
	if id <= 0 {
		return cause
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), provisionCleanupTimeout)
	defer cancel()
	if err := h.client.DeleteServer(cleanupCtx, id); err != nil {
		return errors.Join(cause, fmt.Errorf("hetzner: clean up server %d after failed provision: %w", id, err))
	}
	return cause
}

// Destroy permanently deletes a server by numeric provider ID. The concrete
// client treats an already-absent server as success.
func (h *Hetzner) Destroy(ctx context.Context, id string) error {
	if h.client == nil {
		return errors.New("hetzner: provider is not configured")
	}
	n, err := positiveID(id, "server")
	if err != nil {
		return err
	}
	if err := h.client.DeleteServer(ctx, n); err != nil {
		return fmt.Errorf("hetzner: delete server %d: %w", n, err)
	}
	return nil
}

// List returns only normal workers with the exact deployment ownership tag;
// image builders have a different role and cannot be adopted by reconcile.
func (h *Hetzner) List(ctx context.Context, tag string) ([]provider.Instance, error) {
	return h.listServersByRole(ctx, tag, roleWorker)
}

// ListBuilders exposes crash-leaked image builders without mixing them into
// the dispatchable worker list. The core uses this capability only for
// recovery and cleanup.
func (h *Hetzner) ListBuilders(ctx context.Context, tag string) ([]provider.Instance, error) {
	return h.listServersByRole(ctx, tag, roleBuilder)
}

// PromoteBuilder converts one fully labelled owned builder into a normal
// worker by replacing only its role label. All ownership and future metadata
// chunks are carried through byte-for-byte.
func (h *Hetzner) PromoteBuilder(ctx context.Context, id, tag string) error {
	if h.client == nil {
		return errors.New("hetzner: provider is not configured")
	}
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return errors.New("hetzner: builder promotion tag must not be empty")
	}
	serverID, err := positiveID(id, "server")
	if err != nil {
		return err
	}
	before, err := h.client.GetServer(ctx, serverID)
	if err != nil {
		return fmt.Errorf("hetzner: get server %d before builder promotion: %w", serverID, err)
	}
	if !hasOwnership(before.Labels, tag, roleBuilder) {
		return fmt.Errorf("hetzner: server %d is not an owned builder for tag %q", serverID, tag)
	}
	promotedLabels := cloneLabels(before.Labels)
	promotedLabels[labelRole] = roleWorker
	after, err := h.client.UpdateServerLabels(ctx, serverID, promotedLabels)
	if err != nil {
		return fmt.Errorf("hetzner: promote builder %d labels: %w", serverID, err)
	}
	if after.ID != before.ID || !maps.Equal(after.Labels, promotedLabels) {
		return fmt.Errorf("hetzner: promote builder %d returned mismatched server labels", serverID)
	}
	return nil
}

func (h *Hetzner) listServersByRole(ctx context.Context, tag, role string) ([]provider.Instance, error) {
	if h.client == nil {
		return nil, errors.New("hetzner: provider is not configured")
	}
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return nil, errors.New("hetzner: list tag must not be empty")
	}
	servers, err := h.client.ListServers(ctx, ownershipSelector(tag, role))
	if err != nil {
		return nil, fmt.Errorf("hetzner: list servers: %w", err)
	}
	out := make([]provider.Instance, 0, len(servers))
	for _, server := range servers {
		if !hasOwnership(server.Labels, tag, role) {
			continue
		}
		inst, convErr := toInstance(server, tag, h.cfg.NetworkID != 0)
		if convErr != nil {
			return nil, convErr
		}
		out = append(out, inst)
	}
	return out, nil
}

// Reset rebuilds an owned worker from an owned managed snapshot and preserves
// the allocation's original CreatedAt billing anchor.
func (h *Hetzner) Reset(ctx context.Context, id string, spec provider.ResetSpec) (provider.Instance, error) {
	if h.client == nil {
		return provider.Instance{}, errors.New("hetzner: provider is not configured")
	}
	serverID, err := positiveID(id, "server")
	if err != nil {
		return provider.Instance{}, err
	}
	if strings.TrimSpace(spec.ImageID) == "" {
		return provider.Instance{}, errors.New("hetzner: reset image_id must not be empty")
	}
	before, err := h.client.GetServer(ctx, serverID)
	if err != nil {
		return provider.Instance{}, fmt.Errorf("hetzner: get server %d before rebuild: %w", serverID, err)
	}
	tag, err := decodeLabelChunks(before.Labels, labelTagParts, labelTagPrefix)
	if err != nil || !hasOwnership(before.Labels, tag, roleWorker) {
		return provider.Instance{}, fmt.Errorf("hetzner: server %d is not an owned worker", serverID)
	}
	image, err := h.ownedImage(ctx, spec.ImageID)
	if err != nil {
		return provider.Instance{}, err
	}
	if image.Labels[labelRole] != roleImage || !sameOwnership(before.Labels, image.Labels) {
		return provider.Instance{}, fmt.Errorf("hetzner: image %s is not a managed snapshot for server %d", spec.ImageID, serverID)
	}
	userData, err := renderUserData(spec.UserData, spec.AuthorizedKey)
	if err != nil {
		return provider.Instance{}, err
	}
	after, err := h.client.RebuildServer(ctx, serverID, image.ID, userData)
	if err != nil {
		return provider.Instance{}, fmt.Errorf("hetzner: rebuild server %d: %w", serverID, err)
	}
	// Rebuild is an action on the same paid allocation. Keep the original
	// provider timestamp even if an SDK response ever reports a newer value.
	after.CreatedAt = before.CreatedAt
	inst, err := toInstance(after, tag, h.cfg.NetworkID != 0)
	if err != nil {
		return provider.Instance{}, err
	}
	return inst, nil
}

// CreateImage powers an owned builder off and verifies it is stopped before
// asking Hetzner to capture a snapshot. This keeps a crash-consistent root
// disk without requiring provider-specific shutdown logic in the core.
func (h *Hetzner) CreateImage(ctx context.Context, spec provider.ImageSpec) (provider.ManagedImage, error) {
	if h.client == nil {
		return provider.ManagedImage{}, errors.New("hetzner: provider is not configured")
	}
	if strings.TrimSpace(spec.Tag) == "" || strings.TrimSpace(spec.Name) == "" ||
		strings.TrimSpace(spec.SourceInstanceID) == "" || strings.TrimSpace(spec.Fingerprint) == "" {
		return provider.ManagedImage{}, errors.New("hetzner: image spec requires tag, name, source_instance_id, and fingerprint")
	}
	sourceID, err := positiveID(spec.SourceInstanceID, "server")
	if err != nil {
		return provider.ManagedImage{}, err
	}
	source, err := h.client.GetServer(ctx, sourceID)
	if err != nil {
		return provider.ManagedImage{}, fmt.Errorf("hetzner: get snapshot builder %d: %w", sourceID, err)
	}
	if !hasOwnership(source.Labels, spec.Tag, roleBuilder) {
		return provider.ManagedImage{}, fmt.Errorf("hetzner: server %d is not an owned builder for tag %q", sourceID, spec.Tag)
	}
	labels, err := snapshotLabels(spec.Tag, spec.Fingerprint)
	if err != nil {
		return provider.ManagedImage{}, fmt.Errorf("hetzner: snapshot labels: %w", err)
	}
	// The dispatcher returns only after sysprep and sync complete. The
	// provider can now shut down immediately without racing credential scrub;
	// PowerOffServer returns only after cloud state is confirmed off.
	if err := h.client.PowerOffServer(ctx, sourceID); err != nil {
		return provider.ManagedImage{}, fmt.Errorf("hetzner: power off prepared snapshot builder %d: %w", sourceID, err)
	}
	image, err := h.client.CreateSnapshot(ctx, CreateSnapshotRequest{
		SourceServerID: sourceID,
		Name:           spec.Name,
		Labels:         labels,
	})
	if err != nil {
		return provider.ManagedImage{}, fmt.Errorf("hetzner: create snapshot from server %d: %w", sourceID, err)
	}
	return toManagedImage(image)
}

// DeleteImage removes a daemon-managed snapshot by numeric provider ID.
func (h *Hetzner) DeleteImage(ctx context.Context, id string) error {
	if h.client == nil {
		return errors.New("hetzner: provider is not configured")
	}
	n, err := positiveID(id, "image")
	if err != nil {
		return err
	}
	if err := h.client.DeleteImage(ctx, n); err != nil {
		return fmt.Errorf("hetzner: delete image %d: %w", n, err)
	}
	return nil
}

// ListImages returns only snapshots owned by the exact deployment tag.
func (h *Hetzner) ListImages(ctx context.Context, tag string) ([]provider.ManagedImage, error) {
	if h.client == nil {
		return nil, errors.New("hetzner: provider is not configured")
	}
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return nil, errors.New("hetzner: list image tag must not be empty")
	}
	images, err := h.client.ListImages(ctx, ownershipSelector(tag, roleImage))
	if err != nil {
		return nil, fmt.Errorf("hetzner: list snapshots: %w", err)
	}
	out := make([]provider.ManagedImage, 0, len(images))
	for _, image := range images {
		if !hasOwnership(image.Labels, tag, roleImage) {
			continue
		}
		managed, convErr := toManagedImage(image)
		if convErr != nil {
			// Keep an interrupted/partially labelled owned image visible to
			// rotation as stale instead of wedging all image discovery.
			managed = provider.ManagedImage{
				ID: strconv.FormatInt(image.ID, 10), Name: image.Name,
				CreatedAt: image.CreatedAt, SizeBytes: image.SizeBytes,
			}
		}
		out = append(out, managed)
	}
	return out, nil
}

func (h *Hetzner) ownedImage(ctx context.Context, id string) (Image, error) {
	n, err := positiveID(id, "image")
	if err != nil {
		return Image{}, err
	}
	image, err := h.client.GetImage(ctx, n)
	if err != nil {
		return Image{}, fmt.Errorf("hetzner: get image %d: %w", n, err)
	}
	return image, nil
}

func positiveID(raw, kind string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || n <= 0 {
		if err == nil {
			err = errors.New("ID must be positive")
		}
		return 0, fmt.Errorf("hetzner: bad %s ID %q: %w", kind, raw, err)
	}
	return n, nil
}

func toInstance(server Server, tag string, includePrivateIP bool) (provider.Instance, error) {
	if server.ID <= 0 {
		return provider.Instance{}, errors.New("hetzner: server response has no ID")
	}
	if server.CreatedAt.IsZero() {
		return provider.Instance{}, fmt.Errorf("hetzner: server %d has no created timestamp", server.ID)
	}
	privateIP := ""
	if includePrivateIP {
		privateIP = server.PrivateIPv4
	}
	return provider.Instance{
		ID:        strconv.FormatInt(server.ID, 10),
		Name:      server.Name,
		IPv4:      server.PublicIPv4,
		VPCIPv4:   privateIP,
		CreatedAt: server.CreatedAt,
		Tag:       tag,
	}, nil
}

func toManagedImage(image Image) (provider.ManagedImage, error) {
	if image.ID <= 0 {
		return provider.ManagedImage{}, errors.New("hetzner: image response has no ID")
	}
	fingerprint, err := decodeLabelChunks(image.Labels, labelFingerprintParts, labelFingerprintPrefix)
	if err != nil {
		return provider.ManagedImage{}, fmt.Errorf("hetzner: image %d fingerprint labels: %w", image.ID, err)
	}
	return provider.ManagedImage{
		ID:          strconv.FormatInt(image.ID, 10),
		Name:        image.Name,
		Fingerprint: fingerprint,
		CreatedAt:   image.CreatedAt,
		SizeBytes:   image.SizeBytes,
	}, nil
}

// BillingModel reports Hetzner's whole-hour round-up behavior so the core
// retains idle workers until the paid-hour boundary.
func (*Hetzner) BillingModel() provider.BillingModel {
	return provider.BillingHourlyRoundUp
}

// Info returns stable non-secret configuration details for diagnostics.
func (h *Hetzner) Info(_ context.Context) map[string]string {
	firewallIDs := make([]string, 0, len(h.cfg.FirewallIDs))
	for _, id := range h.cfg.FirewallIDs {
		firewallIDs = append(firewallIDs, strconv.FormatInt(id, 10))
	}
	return map[string]string{
		"location":     h.cfg.Location,
		"locations":    strings.Join(h.cfg.Locations, ","),
		"image":        h.cfg.Image,
		"network_id":   strconv.FormatInt(h.cfg.NetworkID, 10),
		"firewall_ids": strings.Join(firewallIDs, ","),
		"tag":          h.tag,
	}
}
