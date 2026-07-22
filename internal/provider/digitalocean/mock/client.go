// Package mock provides a hand-written DigitalOcean API client fake for
// hermetic provider tests.
package mock

import (
	"context"
	"errors"
	"sync"

	"github.com/digitalocean/godo"

	"github.com/hstern/fj-bellows/internal/provider/digitalocean"
)

// FirewallCall records one firewall attachment request.
type FirewallCall struct {
	FirewallID string
	DropletID  int
}

// Client implements digitalocean.Client with configurable function fields.
// Calls are recorded under a mutex and snapshot accessors return defensive
// copies, making it safe to use from concurrent provider tests. Configure the
// function fields before starting concurrent calls.
type Client struct {
	CreateDropletFn        func(context.Context, godo.DropletCreateRequest) (godo.Droplet, error)
	GetDropletFn           func(context.Context, int) (godo.Droplet, error)
	DeleteDropletFn        func(context.Context, int) error
	ListDropletsByTagFn    func(context.Context, string) ([]godo.Droplet, error)
	AddDropletToFirewallFn func(context.Context, string, int) error
	FindSizeFn             func(context.Context, string) (godo.Size, error)

	mu            sync.Mutex
	createCalls   []godo.DropletCreateRequest
	getCalls      []int
	deleteCalls   []int
	listCalls     []string
	firewallCalls []FirewallCall
	findSizeCalls []string
}

var _ digitalocean.Client = (*Client)(nil)

// CreateDroplet records req and invokes CreateDropletFn.
func (c *Client) CreateDroplet(ctx context.Context, req godo.DropletCreateRequest) (godo.Droplet, error) {
	c.mu.Lock()
	c.createCalls = append(c.createCalls, cloneCreateRequest(req))
	fn := c.CreateDropletFn
	c.mu.Unlock()
	if fn == nil {
		return godo.Droplet{}, errors.New("digitalocean mock: unexpected CreateDroplet call")
	}
	return fn(ctx, req)
}

// GetDroplet records id and invokes GetDropletFn.
func (c *Client) GetDroplet(ctx context.Context, id int) (godo.Droplet, error) {
	c.mu.Lock()
	c.getCalls = append(c.getCalls, id)
	fn := c.GetDropletFn
	c.mu.Unlock()
	if fn == nil {
		return godo.Droplet{}, errors.New("digitalocean mock: unexpected GetDroplet call")
	}
	return fn(ctx, id)
}

// DeleteDroplet records id and invokes DeleteDropletFn.
func (c *Client) DeleteDroplet(ctx context.Context, id int) error {
	c.mu.Lock()
	c.deleteCalls = append(c.deleteCalls, id)
	fn := c.DeleteDropletFn
	c.mu.Unlock()
	if fn == nil {
		return errors.New("digitalocean mock: unexpected DeleteDroplet call")
	}
	return fn(ctx, id)
}

// ListDropletsByTag records tag and invokes ListDropletsByTagFn.
func (c *Client) ListDropletsByTag(ctx context.Context, tag string) ([]godo.Droplet, error) {
	c.mu.Lock()
	c.listCalls = append(c.listCalls, tag)
	fn := c.ListDropletsByTagFn
	c.mu.Unlock()
	if fn == nil {
		return nil, errors.New("digitalocean mock: unexpected ListDropletsByTag call")
	}
	return fn(ctx, tag)
}

// AddDropletToFirewall records the attachment and invokes
// AddDropletToFirewallFn.
func (c *Client) AddDropletToFirewall(ctx context.Context, firewallID string, dropletID int) error {
	c.mu.Lock()
	c.firewallCalls = append(c.firewallCalls, FirewallCall{FirewallID: firewallID, DropletID: dropletID})
	fn := c.AddDropletToFirewallFn
	c.mu.Unlock()
	if fn == nil {
		return errors.New("digitalocean mock: unexpected AddDropletToFirewall call")
	}
	return fn(ctx, firewallID, dropletID)
}

// FindSize records slug and invokes FindSizeFn.
func (c *Client) FindSize(ctx context.Context, slug string) (godo.Size, error) {
	c.mu.Lock()
	c.findSizeCalls = append(c.findSizeCalls, slug)
	fn := c.FindSizeFn
	c.mu.Unlock()
	if fn == nil {
		return godo.Size{}, errors.New("digitalocean mock: unexpected FindSize call")
	}
	return fn(ctx, slug)
}

// CreateCalls returns a defensive snapshot of create requests.
func (c *Client) CreateCalls() []godo.DropletCreateRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]godo.DropletCreateRequest, len(c.createCalls))
	for i, call := range c.createCalls {
		out[i] = cloneCreateRequest(call)
	}
	return out
}

// GetCalls returns a defensive snapshot of fetched Droplet IDs.
func (c *Client) GetCalls() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.getCalls...)
}

// DeleteCalls returns a defensive snapshot of deleted Droplet IDs.
func (c *Client) DeleteCalls() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.deleteCalls...)
}

// ListCalls returns a defensive snapshot of requested ownership tags.
func (c *Client) ListCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.listCalls...)
}

// FirewallCalls returns a defensive snapshot of firewall attachments.
func (c *Client) FirewallCalls() []FirewallCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]FirewallCall(nil), c.firewallCalls...)
}

// FindSizeCalls returns a defensive snapshot of catalog size lookups.
func (c *Client) FindSizeCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.findSizeCalls...)
}

func cloneCreateRequest(req godo.DropletCreateRequest) godo.DropletCreateRequest {
	req.SSHKeys = append([]godo.DropletCreateSSHKey(nil), req.SSHKeys...)
	req.Volumes = append([]godo.DropletCreateVolume(nil), req.Volumes...)
	req.Tags = append([]string(nil), req.Tags...)
	if req.PublicNetworking != nil {
		value := *req.PublicNetworking
		req.PublicNetworking = &value
	}
	if req.WithDropletAgent != nil {
		value := *req.WithDropletAgent
		req.WithDropletAgent = &value
	}
	return req
}
