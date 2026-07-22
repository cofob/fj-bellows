package digitalocean

import (
	"context"
	"errors"
	"fmt"

	"github.com/digitalocean/godo"
)

const listPageSize = 200

// Client is the narrow DigitalOcean API surface used by the provider. The
// sibling mock package supplies a concurrency-safe in-memory implementation.
type Client interface {
	CreateDroplet(ctx context.Context, req godo.DropletCreateRequest) (godo.Droplet, error)
	GetDroplet(ctx context.Context, id int) (godo.Droplet, error)
	DeleteDroplet(ctx context.Context, id int) error
	ListDropletsByTag(ctx context.Context, tag string) ([]godo.Droplet, error)
	AddDropletToFirewall(ctx context.Context, firewallID string, dropletID int) error
	FindSize(ctx context.Context, slug string) (godo.Size, error)
}

type godoClient struct {
	droplets  godo.DropletsService
	firewalls godo.FirewallsService
	sizes     godo.SizesService
}

var _ Client = (*godoClient)(nil)

func newGodoClient(token string) Client {
	client := godo.NewFromToken(token)
	return &godoClient{
		droplets:  client.Droplets,
		firewalls: client.Firewalls,
		sizes:     client.Sizes,
	}
}

func (c *godoClient) CreateDroplet(ctx context.Context, req godo.DropletCreateRequest) (godo.Droplet, error) {
	droplet, _, err := c.droplets.Create(ctx, &req)
	if err != nil {
		return godo.Droplet{}, err
	}
	if droplet == nil {
		return godo.Droplet{}, errors.New("digitalocean API returned an empty droplet")
	}
	return *droplet, nil
}

func (c *godoClient) GetDroplet(ctx context.Context, id int) (godo.Droplet, error) {
	droplet, _, err := c.droplets.Get(ctx, id)
	if err != nil {
		return godo.Droplet{}, err
	}
	if droplet == nil {
		return godo.Droplet{}, errors.New("digitalocean API returned an empty droplet")
	}
	return *droplet, nil
}

func (c *godoClient) DeleteDroplet(ctx context.Context, id int) error {
	_, err := c.droplets.Delete(ctx, id)
	return err
}

func (c *godoClient) ListDropletsByTag(ctx context.Context, tag string) ([]godo.Droplet, error) {
	page := 1
	var out []godo.Droplet
	for {
		droplets, resp, err := c.droplets.ListByTag(ctx, tag, &godo.ListOptions{Page: page, PerPage: listPageSize})
		if err != nil {
			return nil, err
		}
		out = append(out, droplets...)
		if resp == nil || resp.Links == nil || resp.Links.IsLastPage() {
			return out, nil
		}
		current, err := resp.Links.CurrentPage()
		if err != nil {
			return nil, fmt.Errorf("read DigitalOcean droplet pagination: %w", err)
		}
		page = current + 1
	}
}

func (c *godoClient) AddDropletToFirewall(ctx context.Context, firewallID string, dropletID int) error {
	_, err := c.firewalls.AddDroplets(ctx, firewallID, dropletID)
	return err
}

func (c *godoClient) FindSize(ctx context.Context, slug string) (godo.Size, error) {
	page := 1
	for {
		sizes, resp, err := c.sizes.List(ctx, &godo.ListOptions{Page: page, PerPage: listPageSize})
		if err != nil {
			return godo.Size{}, err
		}
		for _, size := range sizes {
			if size.Slug == slug {
				return size, nil
			}
		}
		if resp == nil || resp.Links == nil || resp.Links.IsLastPage() {
			return godo.Size{}, fmt.Errorf("DigitalOcean size %q not found", slug)
		}
		current, err := resp.Links.CurrentPage()
		if err != nil {
			return godo.Size{}, fmt.Errorf("read DigitalOcean size pagination: %w", err)
		}
		page = current + 1
	}
}
