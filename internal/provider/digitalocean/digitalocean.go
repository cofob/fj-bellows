// Package digitalocean implements ephemeral DigitalOcean Droplets for the
// provider abstraction.
package digitalocean

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/digitalocean/godo"
	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/provider"
)

const (
	maxUserDataBytes        = 64 * 1024
	provisionCleanupTimeout = 30 * time.Second
	provisionReadyTimeout   = 3 * time.Minute
	provisionPollInterval   = 2 * time.Second
	dropletStatusActive     = "active"
)

type config struct {
	Token           string           `yaml:"token"`
	Region          string           `yaml:"region"`
	Image           string           `yaml:"image"`
	VPCUUID         string           `yaml:"vpc_uuid"`
	SSHKeyIDs       []int            `yaml:"ssh_key_ids"`
	FirewallID      string           `yaml:"firewall_id"`
	PricingOverride *pricingOverride `yaml:"pricing_override"`
}

func (c *config) normalizeAndValidate() error {
	c.Token = strings.TrimSpace(c.Token)
	c.Region = strings.TrimSpace(c.Region)
	c.Image = strings.TrimSpace(c.Image)
	c.VPCUUID = strings.TrimSpace(c.VPCUUID)
	c.FirewallID = strings.TrimSpace(c.FirewallID)

	var missing []string
	if c.Token == "" {
		missing = append(missing, "token")
	}
	if c.Region == "" {
		missing = append(missing, "region")
	}
	if c.Image == "" {
		missing = append(missing, "image")
	}
	if len(missing) != 0 {
		return fmt.Errorf("digitalocean: provider config missing: %s", strings.Join(missing, ", "))
	}

	seenKeys := make(map[int]struct{}, len(c.SSHKeyIDs))
	for _, id := range c.SSHKeyIDs {
		if id <= 0 {
			return fmt.Errorf("digitalocean: provider config: ssh_key_ids must contain positive IDs, got %d", id)
		}
		if _, ok := seenKeys[id]; ok {
			return fmt.Errorf("digitalocean: provider config: duplicate ssh_key_ids entry %d", id)
		}
		seenKeys[id] = struct{}{}
	}

	if c.PricingOverride != nil {
		if err := c.PricingOverride.normalizeAndValidate(); err != nil {
			return fmt.Errorf("digitalocean: provider config: pricing_override: %w", err)
		}
	}
	return nil
}

// DigitalOcean provisions tagged Droplets through the official godo client.
// The concrete SDK is hidden behind Client so tests and callers never need to
// reach the public API.
type DigitalOcean struct {
	cfg    config
	client Client
	tag    string
	now    func() time.Time
}

var (
	_ provider.Provider     = (*DigitalOcean)(nil)
	_ provider.InfoProvider = (*DigitalOcean)(nil)
	_ provider.Pricer       = (*DigitalOcean)(nil)
)

func init() {
	provider.Register("digitalocean", func() provider.Provider { return &DigitalOcean{} })
}

// NewWithClient constructs a provider with an injected API client. It is
// primarily useful to composition tests and alternate godo transports;
// Configure still validates the complete provider configuration.
func NewWithClient(client Client) *DigitalOcean {
	return &DigitalOcean{client: client, now: time.Now}
}

// Configure decodes the opaque provider block and prepares the godo adapter.
// It deliberately performs no cloud mutations.
func (d *DigitalOcean) Configure(_ context.Context, tag string, node yaml.Node) error {
	var cfg config
	if err := provider.DecodeConfig(node, &cfg); err != nil {
		return fmt.Errorf("digitalocean: decode provider config: %w", err)
	}
	if err := cfg.normalizeAndValidate(); err != nil {
		return err
	}

	d.cfg = cfg
	d.tag = tag
	if d.client == nil {
		d.client = newGodoClient(cfg.Token)
	}
	if d.now == nil {
		d.now = time.Now
	}
	return nil
}

// Provision creates a Droplet with public IPv4 networking, the tier-selected
// size, provider/tier cloud-init, and the deployment ownership tag.
func (d *DigitalOcean) Provision(ctx context.Context, spec provider.Spec) (provider.Instance, error) {
	if d.client == nil {
		return provider.Instance{}, errors.New("digitalocean: provider is not configured")
	}
	if err := validateSpec(spec); err != nil {
		return provider.Instance{}, err
	}

	imageRef := d.cfg.Image
	if strings.TrimSpace(spec.ImageID) != "" {
		imageRef = spec.ImageID
	}
	image, err := createImage(imageRef)
	if err != nil {
		return provider.Instance{}, err
	}
	userData, err := renderUserData(spec.UserData, spec.AuthorizedKey)
	if err != nil {
		return provider.Instance{}, fmt.Errorf("digitalocean: render user-data: %w", err)
	}

	sshKeys := make([]godo.DropletCreateSSHKey, 0, len(d.cfg.SSHKeyIDs))
	for _, id := range d.cfg.SSHKeyIDs {
		sshKeys = append(sshKeys, godo.DropletCreateSSHKey{ID: id})
	}
	publicNetworking := true
	req := godo.DropletCreateRequest{
		Name:             spec.Name,
		Region:           d.cfg.Region,
		Size:             spec.InstanceType,
		Image:            image,
		SSHKeys:          sshKeys,
		UserData:         userData,
		Tags:             []string{spec.Tag},
		VPCUUID:          d.cfg.VPCUUID,
		PublicNetworking: &publicNetworking,
	}

	droplet, err := d.client.CreateDroplet(ctx, req)
	if err != nil {
		return provider.Instance{}, fmt.Errorf("digitalocean: create droplet: %w", err)
	}
	if d.cfg.FirewallID != "" {
		if err := d.client.AddDropletToFirewall(ctx, d.cfg.FirewallID, droplet.ID); err != nil {
			return provider.Instance{}, d.cleanupFailedProvision(ctx, droplet.ID,
				fmt.Errorf("digitalocean: attach droplet %d to firewall %q: %w", droplet.ID, d.cfg.FirewallID, err))
		}
	}
	droplet, err = d.waitForReadyDroplet(ctx, droplet)
	if err != nil {
		return provider.Instance{}, d.cleanupFailedProvision(ctx, droplet.ID, err)
	}

	inst, err := toInstance(droplet, spec.Tag, d.cfg.VPCUUID != "")
	if err != nil {
		return provider.Instance{}, d.cleanupFailedProvision(ctx, droplet.ID, err)
	}
	return inst, nil
}

func (d *DigitalOcean) waitForReadyDroplet(ctx context.Context, droplet godo.Droplet) (godo.Droplet, error) {
	waitCtx, cancel := context.WithTimeout(ctx, provisionReadyTimeout)
	defer cancel()
	id := droplet.ID
	for {
		if droplet.Status == dropletStatusActive && publicDropletIPv4(droplet) != "" {
			return droplet, nil
		}
		refreshed, err := d.client.GetDroplet(waitCtx, id)
		if err != nil {
			if waitCtx.Err() != nil {
				return droplet, fmt.Errorf(
					"digitalocean: wait for droplet %d to become active with public IPv4: %w",
					id, waitCtx.Err(),
				)
			}
			return droplet, fmt.Errorf("digitalocean: get droplet %d while waiting for readiness: %w", id, err)
		}
		if refreshed.ID != id {
			return droplet, fmt.Errorf(
				"digitalocean: get droplet %d returned unexpected droplet ID %d", id, refreshed.ID,
			)
		}
		droplet = refreshed
		if droplet.Status == dropletStatusActive && publicDropletIPv4(droplet) != "" {
			return droplet, nil
		}
		timer := time.NewTimer(provisionPollInterval)
		select {
		case <-waitCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return droplet, fmt.Errorf(
				"digitalocean: wait for droplet %d to become active with public IPv4: %w",
				id, waitCtx.Err(),
			)
		case <-timer.C:
		}
	}
}

func validateSpec(spec provider.Spec) error {
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
		return fmt.Errorf("digitalocean: provision spec missing: %s", strings.Join(missing, ", "))
	}
	if len(spec.UserData) > maxUserDataBytes {
		return fmt.Errorf("digitalocean: user-data is %d bytes; maximum is %d", len(spec.UserData), maxUserDataBytes)
	}
	return nil
}

func createImage(ref string) (godo.DropletCreateImage, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return godo.DropletCreateImage{}, errors.New("digitalocean: image reference is empty")
	}
	if id, err := strconv.Atoi(ref); err == nil {
		if id <= 0 {
			return godo.DropletCreateImage{}, fmt.Errorf("digitalocean: image ID must be positive, got %q", ref)
		}
		return godo.DropletCreateImage{ID: id}, nil
	}
	return godo.DropletCreateImage{Slug: ref}, nil
}

func (d *DigitalOcean) cleanupFailedProvision(ctx context.Context, id int, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), provisionCleanupTimeout)
	defer cancel()
	if err := d.client.DeleteDroplet(cleanupCtx, id); err != nil {
		return errors.Join(cause, fmt.Errorf("digitalocean: clean up droplet %d after failed provision: %w", id, err))
	}
	return cause
}

// Destroy permanently deletes a Droplet by its numeric provider ID.
func (d *DigitalOcean) Destroy(ctx context.Context, id string) error {
	if d.client == nil {
		return errors.New("digitalocean: provider is not configured")
	}
	n, err := strconv.Atoi(id)
	if err != nil || n <= 0 {
		if err == nil {
			err = errors.New("ID must be positive")
		}
		return fmt.Errorf("digitalocean: bad droplet ID %q: %w", id, err)
	}
	if err := d.client.DeleteDroplet(ctx, n); err != nil {
		return fmt.Errorf("digitalocean: delete droplet %d: %w", n, err)
	}
	return nil
}

// List returns every Droplet matched by the exact ownership tag. The godo
// adapter exhausts all result pages before returning.
func (d *DigitalOcean) List(ctx context.Context, tag string) ([]provider.Instance, error) {
	if d.client == nil {
		return nil, errors.New("digitalocean: provider is not configured")
	}
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return nil, errors.New("digitalocean: list tag must not be empty")
	}
	droplets, err := d.client.ListDropletsByTag(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("digitalocean: list droplets by tag %q: %w", tag, err)
	}
	out := make([]provider.Instance, 0, len(droplets))
	for _, droplet := range droplets {
		inst, err := toInstance(droplet, tag, d.cfg.VPCUUID != "")
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, nil
}

func toInstance(droplet godo.Droplet, ownershipTag string, includePrivateIP bool) (provider.Instance, error) {
	created, err := time.Parse(time.RFC3339, droplet.Created)
	if err != nil {
		return provider.Instance{}, fmt.Errorf("digitalocean: droplet %d has invalid created_at %q: %w", droplet.ID, droplet.Created, err)
	}

	publicIP := publicDropletIPv4(droplet)
	if publicIP == "" {
		return provider.Instance{}, fmt.Errorf("digitalocean: droplet %d has no public IPv4", droplet.ID)
	}
	var privateIP string
	if droplet.Networks != nil {
		for _, network := range droplet.Networks.V4 {
			if network.Type == "private" && includePrivateIP && privateIP == "" {
				privateIP = network.IPAddress
			}
		}
	}

	return provider.Instance{
		ID:        strconv.Itoa(droplet.ID),
		Name:      droplet.Name,
		IPv4:      publicIP,
		VPCIPv4:   privateIP,
		CreatedAt: created,
		Tag:       ownershipTag,
	}, nil
}

func publicDropletIPv4(droplet godo.Droplet) string {
	if droplet.Networks == nil {
		return ""
	}
	for _, network := range droplet.Networks.V4 {
		if network.Type == "public" && network.IPAddress != "" {
			return network.IPAddress
		}
	}
	return ""
}

// BillingModel reports DigitalOcean's per-second Droplet billing behavior.
func (*DigitalOcean) BillingModel() provider.BillingModel {
	return provider.BillingPerSecond
}

// Info returns stable, non-secret configuration details for operator
// diagnostics. It performs no API calls.
func (d *DigitalOcean) Info(_ context.Context) map[string]string {
	return map[string]string{
		"region":        d.cfg.Region,
		"image":         d.cfg.Image,
		"vpc_uuid":      d.cfg.VPCUUID,
		"firewall_id":   d.cfg.FirewallID,
		"ssh_key_count": strconv.Itoa(len(d.cfg.SSHKeyIDs)),
		"tag":           d.tag,
	}
}
