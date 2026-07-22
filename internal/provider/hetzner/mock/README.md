# Hetzner provider mocks

This package contains the hand-written, concurrency-safe fake for the narrow
Hetzner API client used by `internal/provider/hetzner`.

Tests configure function fields before use. Every method records calls under a
mutex, and accessors return defensive copies so assertions can safely observe
a provider running in another goroutine.
