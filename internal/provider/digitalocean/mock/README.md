# DigitalOcean provider mocks

This package contains the hand-written, concurrency-safe fake for the narrow
DigitalOcean API client used by `internal/provider/digitalocean`.

Tests configure function fields before use. Every API method records calls
under a mutex, and the accessor methods return defensive snapshots so tests can
assert against a provider running in another goroutine without data races.
