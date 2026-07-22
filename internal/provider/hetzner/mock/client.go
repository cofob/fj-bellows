// Package mock provides a hand-written, concurrency-safe fake for the narrow
// Hetzner provider API client.
package mock

import (
	"context"
	"errors"
	"maps"
	"sync"

	"github.com/hstern/fj-bellows/internal/provider/hetzner"
)

// Call records one API method and its primary numeric/string argument. It is
// useful for asserting cross-method order, such as poweroff before snapshot.
type Call struct {
	Method   string
	ID       int64
	Selector string
}

// Client implements hetzner.Client through configurable function fields.
// Configure function fields before concurrent use; method calls and accessor
// snapshots are protected by a mutex.
type Client struct {
	CreateServerFn       func(context.Context, hetzner.CreateServerRequest) (hetzner.Server, error)
	GetServerFn          func(context.Context, int64) (hetzner.Server, error)
	UpdateServerLabelsFn func(context.Context, int64, map[string]string) (hetzner.Server, error)
	DeleteServerFn       func(context.Context, int64) error
	ListServersFn        func(context.Context, string) ([]hetzner.Server, error)
	PowerOffServerFn     func(context.Context, int64) error
	RebuildServerFn      func(context.Context, int64, int64, string) (hetzner.Server, error)
	CreateSnapshotFn     func(context.Context, hetzner.CreateSnapshotRequest) (hetzner.Image, error)
	GetImageFn           func(context.Context, int64) (hetzner.Image, error)
	DeleteImageFn        func(context.Context, int64) error
	ListImagesFn         func(context.Context, string) ([]hetzner.Image, error)
	GetPricingFn         func(context.Context) (hetzner.Catalog, error)

	mu                      sync.Mutex
	calls                   []Call
	createServerCalls       []hetzner.CreateServerRequest
	updateServerLabelsCalls []UpdateServerLabelsCall
	rebuildServerCalls      []RebuildServerCall
	createSnapshotCalls     []hetzner.CreateSnapshotRequest
}

var _ hetzner.Client = (*Client)(nil)

// RebuildServerCall records a rebuild request.
type RebuildServerCall struct {
	ServerID int64
	ImageID  int64
	UserData string
}

// UpdateServerLabelsCall records one exact label replacement request.
type UpdateServerLabelsCall struct {
	ServerID int64
	Labels   map[string]string
}

// CreateServer records and delegates a server create.
func (c *Client) CreateServer(ctx context.Context, req hetzner.CreateServerRequest) (hetzner.Server, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "CreateServer"})
	c.createServerCalls = append(c.createServerCalls, cloneCreateServerRequest(req))
	fn := c.CreateServerFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Server{}, errors.New("hetzner mock: unexpected CreateServer call")
	}
	return fn(ctx, req)
}

// GetServer records and delegates a server lookup.
func (c *Client) GetServer(ctx context.Context, id int64) (hetzner.Server, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "GetServer", ID: id})
	fn := c.GetServerFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Server{}, errors.New("hetzner mock: unexpected GetServer call")
	}
	return fn(ctx, id)
}

// UpdateServerLabels records and delegates a complete server-label update.
func (c *Client) UpdateServerLabels(
	ctx context.Context,
	id int64,
	labels map[string]string,
) (hetzner.Server, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "UpdateServerLabels", ID: id})
	c.updateServerLabelsCalls = append(c.updateServerLabelsCalls, UpdateServerLabelsCall{
		ServerID: id,
		Labels:   cloneLabels(labels),
	})
	fn := c.UpdateServerLabelsFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Server{}, errors.New("hetzner mock: unexpected UpdateServerLabels call")
	}
	return fn(ctx, id, labels)
}

// DeleteServer records and delegates a server deletion.
func (c *Client) DeleteServer(ctx context.Context, id int64) error {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "DeleteServer", ID: id})
	fn := c.DeleteServerFn
	c.mu.Unlock()
	if fn == nil {
		return errors.New("hetzner mock: unexpected DeleteServer call")
	}
	return fn(ctx, id)
}

// ListServers records and delegates a labelled server listing.
func (c *Client) ListServers(ctx context.Context, selector string) ([]hetzner.Server, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "ListServers", Selector: selector})
	fn := c.ListServersFn
	c.mu.Unlock()
	if fn == nil {
		return nil, errors.New("hetzner mock: unexpected ListServers call")
	}
	return fn(ctx, selector)
}

// PowerOffServer records and delegates an API shutdown fallback.
func (c *Client) PowerOffServer(ctx context.Context, id int64) error {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "PowerOffServer", ID: id})
	fn := c.PowerOffServerFn
	c.mu.Unlock()
	if fn == nil {
		return errors.New("hetzner mock: unexpected PowerOffServer call")
	}
	return fn(ctx, id)
}

// RebuildServer records and delegates an in-place rebuild.
func (c *Client) RebuildServer(ctx context.Context, serverID, imageID int64, userData string) (hetzner.Server, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "RebuildServer", ID: serverID})
	c.rebuildServerCalls = append(c.rebuildServerCalls, RebuildServerCall{
		ServerID: serverID, ImageID: imageID, UserData: userData,
	})
	fn := c.RebuildServerFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Server{}, errors.New("hetzner mock: unexpected RebuildServer call")
	}
	return fn(ctx, serverID, imageID, userData)
}

// CreateSnapshot records and delegates a snapshot capture.
func (c *Client) CreateSnapshot(ctx context.Context, req hetzner.CreateSnapshotRequest) (hetzner.Image, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "CreateSnapshot", ID: req.SourceServerID})
	c.createSnapshotCalls = append(c.createSnapshotCalls, cloneCreateSnapshotRequest(req))
	fn := c.CreateSnapshotFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Image{}, errors.New("hetzner mock: unexpected CreateSnapshot call")
	}
	return fn(ctx, req)
}

// GetImage records and delegates an image lookup.
func (c *Client) GetImage(ctx context.Context, id int64) (hetzner.Image, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "GetImage", ID: id})
	fn := c.GetImageFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Image{}, errors.New("hetzner mock: unexpected GetImage call")
	}
	return fn(ctx, id)
}

// DeleteImage records and delegates an image deletion.
func (c *Client) DeleteImage(ctx context.Context, id int64) error {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "DeleteImage", ID: id})
	fn := c.DeleteImageFn
	c.mu.Unlock()
	if fn == nil {
		return errors.New("hetzner mock: unexpected DeleteImage call")
	}
	return fn(ctx, id)
}

// ListImages records and delegates a labelled image listing.
func (c *Client) ListImages(ctx context.Context, selector string) ([]hetzner.Image, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "ListImages", Selector: selector})
	fn := c.ListImagesFn
	c.mu.Unlock()
	if fn == nil {
		return nil, errors.New("hetzner mock: unexpected ListImages call")
	}
	return fn(ctx, selector)
}

// GetPricing records and delegates a catalog lookup.
func (c *Client) GetPricing(ctx context.Context) (hetzner.Catalog, error) {
	c.mu.Lock()
	c.calls = append(c.calls, Call{Method: "GetPricing"})
	fn := c.GetPricingFn
	c.mu.Unlock()
	if fn == nil {
		return hetzner.Catalog{}, errors.New("hetzner mock: unexpected GetPricing call")
	}
	return fn(ctx)
}

// Calls returns a defensive snapshot of all calls in invocation order.
func (c *Client) Calls() []Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Call(nil), c.calls...)
}

// CreateServerCalls returns a defensive snapshot of create requests.
func (c *Client) CreateServerCalls() []hetzner.CreateServerRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]hetzner.CreateServerRequest, len(c.createServerCalls))
	for i, call := range c.createServerCalls {
		out[i] = cloneCreateServerRequest(call)
	}
	return out
}

// UpdateServerLabelsCalls returns defensive copies of label update requests.
func (c *Client) UpdateServerLabelsCalls() []UpdateServerLabelsCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]UpdateServerLabelsCall, len(c.updateServerLabelsCalls))
	for i, call := range c.updateServerLabelsCalls {
		out[i] = UpdateServerLabelsCall{ServerID: call.ServerID, Labels: cloneLabels(call.Labels)}
	}
	return out
}

// RebuildServerCalls returns a defensive snapshot of rebuild requests.
func (c *Client) RebuildServerCalls() []RebuildServerCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RebuildServerCall(nil), c.rebuildServerCalls...)
}

// CreateSnapshotCalls returns a defensive snapshot of snapshot requests.
func (c *Client) CreateSnapshotCalls() []hetzner.CreateSnapshotRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]hetzner.CreateSnapshotRequest, len(c.createSnapshotCalls))
	for i, call := range c.createSnapshotCalls {
		out[i] = cloneCreateSnapshotRequest(call)
	}
	return out
}

func cloneCreateServerRequest(req hetzner.CreateServerRequest) hetzner.CreateServerRequest {
	req.Labels = cloneLabels(req.Labels)
	req.FirewallIDs = append([]int64(nil), req.FirewallIDs...)
	return req
}

func cloneCreateSnapshotRequest(req hetzner.CreateSnapshotRequest) hetzner.CreateSnapshotRequest {
	req.Labels = cloneLabels(req.Labels)
	return req
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	maps.Copy(out, labels)
	return out
}
