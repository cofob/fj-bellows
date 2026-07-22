package hetzner

import (
	"context"
	"time"
)

// Client is the narrow Hetzner Cloud API surface used by the provider. The
// production implementation adapts hcloud-go; tests use the sibling mock
// package and never contact Hetzner.
type Client interface {
	CreateServer(context.Context, CreateServerRequest) (Server, error)
	GetServer(context.Context, int64) (Server, error)
	UpdateServerLabels(context.Context, int64, map[string]string) (Server, error)
	DeleteServer(context.Context, int64) error
	ListServers(context.Context, string) ([]Server, error)
	PowerOffServer(context.Context, int64) error
	RebuildServer(context.Context, int64, int64, string) (Server, error)

	CreateSnapshot(context.Context, CreateSnapshotRequest) (Image, error)
	GetImage(context.Context, int64) (Image, error)
	DeleteImage(context.Context, int64) error
	ListImages(context.Context, string) ([]Image, error)

	GetPricing(context.Context) (Catalog, error)
}

// CreateServerRequest is the provider's SDK-independent create payload.
type CreateServerRequest struct {
	Name         string
	InstanceType string
	Image        string
	Location     string
	UserData     string
	Labels       map[string]string
	NetworkID    int64
	FirewallIDs  []int64
}

// CreateSnapshotRequest describes an owned snapshot capture.
type CreateSnapshotRequest struct {
	SourceServerID int64
	Name           string
	Labels         map[string]string
}

// Server is the subset of a Hetzner server needed by the provider.
type Server struct {
	ID          int64
	Name        string
	PublicIPv4  string
	PrivateIPv4 string
	CreatedAt   time.Time
	Labels      map[string]string
}

// Image is the subset of a Hetzner image needed by managed snapshot logic.
type Image struct {
	ID        int64
	Name      string
	CreatedAt time.Time
	SizeBytes int64
	Labels    map[string]string
}

// Catalog is a normalized view of Hetzner's public list-price response.
// Decimal amounts remain strings until the provider converts them to fixed
// point nanounits.
type Catalog struct {
	Currency        string
	SnapshotGBMonth string
	ServerTypes     []ServerTypePrice
}

// ServerTypePrice is one server type's price at one location.
type ServerTypePrice struct {
	InstanceType string
	Location     string
	PerHour      string
	PerMonth     string
}
