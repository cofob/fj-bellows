package hetzner

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"time"

	"github.com/hetznercloud/hcloud-go/v2/hcloud"
)

const (
	hcloudPollInterval   = time.Second
	hcloudCleanupTimeout = 30 * time.Second
	bytesPerCloudGB      = 1_000_000_000
)

type hcloudClient struct {
	client *hcloud.Client
}

var _ Client = (*hcloudClient)(nil)

func newHCloudClient(token string) Client {
	return &hcloudClient{client: hcloud.NewClient(hcloud.WithToken(token))}
}

func (c *hcloudClient) CreateServer(ctx context.Context, req CreateServerRequest) (Server, error) {
	image, err := hcloudImageRef(req.Image)
	if err != nil {
		return Server{}, err
	}
	start := true
	opts := hcloud.ServerCreateOpts{
		Name:             req.Name,
		ServerType:       &hcloud.ServerType{Name: req.InstanceType},
		Image:            image,
		Location:         &hcloud.Location{Name: req.Location},
		UserData:         req.UserData,
		StartAfterCreate: &start,
		Labels:           cloneLabels(req.Labels),
		PublicNet: &hcloud.ServerCreatePublicNet{
			EnableIPv4: true,
			EnableIPv6: false,
		},
	}
	if req.NetworkID != 0 {
		opts.Networks = []*hcloud.Network{{ID: req.NetworkID}}
	}
	for _, id := range req.FirewallIDs {
		opts.Firewalls = append(opts.Firewalls, &hcloud.ServerCreateFirewall{
			Firewall: hcloud.Firewall{ID: id},
		})
	}
	result, _, err := c.client.Server.Create(ctx, opts)
	if err != nil {
		return Server{}, err
	}
	if result.Server == nil {
		return Server{}, errors.New("Hetzner API returned an empty server")
	}
	if err := c.waitActions(ctx, result.Action, result.NextActions...); err != nil {
		return Server{}, c.cleanupCreateFailure(ctx, result.Server.ID, fmt.Errorf("wait for server create: %w", err))
	}
	server, _, err := c.client.Server.GetByID(ctx, result.Server.ID)
	if err != nil {
		return Server{}, c.cleanupCreateFailure(ctx, result.Server.ID, fmt.Errorf("refresh created server: %w", err))
	}
	if server == nil {
		return Server{}, c.cleanupCreateFailure(ctx, result.Server.ID, errors.New("created server disappeared"))
	}
	return serverFromHCloud(server), nil
}

func (c *hcloudClient) cleanupCreateFailure(ctx context.Context, id int64, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hcloudCleanupTimeout)
	defer cancel()
	if err := c.DeleteServer(cleanupCtx, id); err != nil {
		return errors.Join(cause, fmt.Errorf("clean up server %d: %w", id, err))
	}
	return cause
}

func (c *hcloudClient) GetServer(ctx context.Context, id int64) (Server, error) {
	server, _, err := c.client.Server.GetByID(ctx, id)
	if err != nil {
		return Server{}, err
	}
	if server == nil {
		return Server{}, fmt.Errorf("server %d not found", id)
	}
	return serverFromHCloud(server), nil
}

func (c *hcloudClient) UpdateServerLabels(ctx context.Context, id int64, labels map[string]string) (Server, error) {
	server, _, err := c.client.Server.GetByID(ctx, id)
	if err != nil {
		return Server{}, err
	}
	if server == nil {
		return Server{}, fmt.Errorf("server %d not found", id)
	}
	updated, _, err := c.client.Server.Update(ctx, server, hcloud.ServerUpdateOpts{Labels: cloneLabels(labels)})
	if err != nil {
		return Server{}, err
	}
	if updated == nil {
		return Server{}, errors.New("Hetzner API returned an empty updated server")
	}
	return serverFromHCloud(updated), nil
}

func (c *hcloudClient) DeleteServer(ctx context.Context, id int64) error {
	server, _, err := c.client.Server.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if server == nil {
		return nil
	}
	result, _, err := c.client.Server.DeleteWithResult(ctx, server)
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("Hetzner API returned an empty delete action")
	}
	return c.waitActions(ctx, result.Action)
}

func (c *hcloudClient) ListServers(ctx context.Context, selector string) ([]Server, error) {
	servers, err := c.client.Server.AllWithOpts(ctx, hcloud.ServerListOpts{
		ListOpts: hcloud.ListOpts{LabelSelector: selector},
	})
	if err != nil {
		return nil, err
	}
	out := make([]Server, 0, len(servers))
	for _, server := range servers {
		out = append(out, serverFromHCloud(server))
	}
	return out, nil
}

func (c *hcloudClient) PowerOffServer(ctx context.Context, id int64) error {
	server, _, err := c.client.Server.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if server == nil {
		return fmt.Errorf("server %d not found", id)
	}
	if server.Status == hcloud.ServerStatusOff {
		return nil
	}

	action, _, shutdownErr := c.client.Server.Shutdown(ctx, server)
	if shutdownErr == nil {
		shutdownErr = c.waitActions(ctx, action)
	}
	if shutdownErr != nil {
		if ctx.Err() != nil {
			return errors.Join(shutdownErr, ctx.Err())
		}
		action, _, err = c.client.Server.Poweroff(ctx, server)
		if err != nil {
			return errors.Join(shutdownErr, fmt.Errorf("hard poweroff fallback: %w", err))
		}
		if err := c.waitActions(ctx, action); err != nil {
			return errors.Join(shutdownErr, fmt.Errorf("wait for hard poweroff fallback: %w", err))
		}
	}

	for {
		current, _, err := c.client.Server.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if current == nil {
			return fmt.Errorf("server %d disappeared while powering off", id)
		}
		if current.Status == hcloud.ServerStatusOff {
			return nil
		}
		timer := time.NewTimer(hcloudPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *hcloudClient) RebuildServer(ctx context.Context, id, imageID int64, userData string) (Server, error) {
	server, _, err := c.client.Server.GetByID(ctx, id)
	if err != nil {
		return Server{}, err
	}
	if server == nil {
		return Server{}, fmt.Errorf("server %d not found", id)
	}
	result, _, err := c.client.Server.RebuildWithResult(ctx, server, hcloud.ServerRebuildOpts{
		Image:    &hcloud.Image{ID: imageID},
		UserData: &userData,
	})
	if err != nil {
		return Server{}, err
	}
	if err := c.waitActions(ctx, result.Action); err != nil {
		return Server{}, err
	}
	server, _, err = c.client.Server.GetByID(ctx, id)
	if err != nil {
		return Server{}, err
	}
	if server == nil {
		return Server{}, fmt.Errorf("server %d disappeared after rebuild", id)
	}
	return serverFromHCloud(server), nil
}

func (c *hcloudClient) CreateSnapshot(ctx context.Context, req CreateSnapshotRequest) (Image, error) {
	server, _, err := c.client.Server.GetByID(ctx, req.SourceServerID)
	if err != nil {
		return Image{}, err
	}
	if server == nil {
		return Image{}, fmt.Errorf("server %d not found", req.SourceServerID)
	}
	result, _, err := c.client.Server.CreateImage(ctx, server, &hcloud.ServerCreateImageOpts{
		Type:        hcloud.ImageTypeSnapshot,
		Description: &req.Name,
		Labels:      cloneLabels(req.Labels),
	})
	if err != nil {
		return Image{}, err
	}
	if result.Image == nil {
		return Image{}, errors.New("Hetzner API returned an empty snapshot")
	}
	if err := c.waitActions(ctx, result.Action); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hcloudCleanupTimeout)
		defer cancel()
		if _, deleteErr := c.client.Image.Delete(cleanupCtx, result.Image); deleteErr != nil {
			return Image{}, errors.Join(err, fmt.Errorf("clean up failed snapshot %d: %w", result.Image.ID, deleteErr))
		}
		return Image{}, err
	}
	image, _, err := c.client.Image.GetByID(ctx, result.Image.ID)
	if err != nil {
		return Image{}, err
	}
	if image == nil {
		return Image{}, fmt.Errorf("snapshot %d disappeared after creation", result.Image.ID)
	}
	return imageFromHCloud(image)
}

func (c *hcloudClient) GetImage(ctx context.Context, id int64) (Image, error) {
	image, _, err := c.client.Image.GetByID(ctx, id)
	if err != nil {
		return Image{}, err
	}
	if image == nil {
		return Image{}, fmt.Errorf("image %d not found", id)
	}
	return imageFromHCloud(image)
}

func (c *hcloudClient) DeleteImage(ctx context.Context, id int64) error {
	image, _, err := c.client.Image.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if image == nil {
		return nil
	}
	_, err = c.client.Image.Delete(ctx, image)
	return err
}

func (c *hcloudClient) ListImages(ctx context.Context, selector string) ([]Image, error) {
	images, err := c.client.Image.AllWithOpts(ctx, hcloud.ImageListOpts{
		ListOpts: hcloud.ListOpts{LabelSelector: selector},
		Type:     []hcloud.ImageType{hcloud.ImageTypeSnapshot},
	})
	if err != nil {
		return nil, err
	}
	out := make([]Image, 0, len(images))
	for _, image := range images {
		converted, err := imageFromHCloud(image)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func (c *hcloudClient) GetPricing(ctx context.Context) (Catalog, error) {
	pricing, _, err := c.client.Pricing.Get(ctx)
	if err != nil {
		return Catalog{}, err
	}
	catalog := Catalog{
		Currency:        pricing.Currency,
		SnapshotGBMonth: pricing.Image.PerGBMonth.Net,
	}
	for _, serverType := range pricing.ServerTypes {
		if serverType.ServerType == nil {
			continue
		}
		for _, price := range serverType.Pricings {
			if price.Location == nil {
				continue
			}
			catalog.ServerTypes = append(catalog.ServerTypes, ServerTypePrice{
				InstanceType: serverType.ServerType.Name,
				Location:     price.Location.Name,
				PerHour:      price.Hourly.Net,
				PerMonth:     price.Monthly.Net,
			})
		}
	}
	return catalog, nil
}

func (c *hcloudClient) waitActions(ctx context.Context, first *hcloud.Action, rest ...*hcloud.Action) error {
	actions := make([]*hcloud.Action, 0, 1+len(rest))
	if first != nil {
		actions = append(actions, first)
	}
	for _, action := range rest {
		if action != nil {
			actions = append(actions, action)
		}
	}
	if len(actions) == 0 {
		return errors.New("Hetzner API returned no action")
	}
	return c.client.Action.WaitFor(ctx, actions...)
}

func hcloudImageRef(ref string) (*hcloud.Image, error) {
	if ref == "" {
		return nil, errors.New("Hetzner image reference is empty")
	}
	if id, err := strconv.ParseInt(ref, 10, 64); err == nil {
		if id <= 0 {
			return nil, fmt.Errorf("Hetzner image ID must be positive, got %q", ref)
		}
		return &hcloud.Image{ID: id}, nil
	}
	return &hcloud.Image{Name: ref}, nil
}

func serverFromHCloud(server *hcloud.Server) Server {
	var publicIPv4, privateIPv4 string
	if !server.PublicNet.IPv4.IsUnspecified() {
		publicIPv4 = server.PublicNet.IPv4.IP.String()
	}
	for _, network := range server.PrivateNet {
		if network.IP != nil && !network.IP.IsUnspecified() {
			privateIPv4 = network.IP.String()
			break
		}
	}
	return Server{
		ID:          server.ID,
		Name:        server.Name,
		PublicIPv4:  publicIPv4,
		PrivateIPv4: privateIPv4,
		CreatedAt:   server.Created,
		Labels:      cloneLabels(server.Labels),
	}
}

func imageFromHCloud(image *hcloud.Image) (Image, error) {
	if image.ImageSize < 0 || float64(image.ImageSize) > float64(math.MaxInt64)/bytesPerCloudGB {
		return Image{}, fmt.Errorf("Hetzner image %d has invalid size %v GB", image.ID, image.ImageSize)
	}
	name := image.Description
	if name == "" {
		name = image.Name
	}
	return Image{
		ID:        image.ID,
		Name:      name,
		CreatedAt: image.Created,
		SizeBytes: int64(math.Round(float64(image.ImageSize) * bytesPerCloudGB)),
		Labels:    cloneLabels(image.Labels),
	}, nil
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	maps.Copy(out, labels)
	return out
}
