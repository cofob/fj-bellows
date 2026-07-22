package control_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	controlv1 "github.com/hstern/fj-bellows/gen/fjbellows/control/v1"
	"github.com/hstern/fj-bellows/internal/control"
	mockctl "github.com/hstern/fj-bellows/internal/control/mock"
)

const (
	selectedTier     = "long"
	selectedProvider = "hetzner-primary"
	selectedDriver   = "hetzner"
	selectedInstance = "long-vm"
)

type selectorBackend struct {
	*mockctl.Backend
	reapCalls atomic.Int32
}

func (*selectorBackend) PoolSnapshotFiltered(tier, providerName string) []control.WorkerView {
	if tier != selectedTier || providerName != selectedProvider {
		return nil
	}
	return []control.WorkerView{{
		Tier: selectedTier, ProviderName: selectedProvider, Driver: selectedDriver,
		InstanceID: selectedInstance, State: "idle",
	}}
}

func (*selectorBackend) ForceProvisionIn(_ context.Context, tier string) (string, error) {
	if tier != selectedTier {
		return "", errors.New("wrong tier forwarded to ForceProvisionIn")
	}
	return selectedInstance, nil
}

func (b *selectorBackend) ForceReapIn(_ context.Context, tier, instanceID string) error {
	if tier != selectedTier || instanceID != selectedInstance {
		return errors.New("wrong selector forwarded to ForceReapIn")
	}
	b.reapCalls.Add(1)
	return nil
}

func (*selectorBackend) ExecOnWorkerIn(
	_ context.Context,
	tier, instanceID, command string,
) ([]byte, []byte, int32, int64, int64, error) {
	if tier != selectedTier || instanceID != selectedInstance || command != "uname -m" {
		return nil, nil, 0, 0, 0, errors.New("wrong selector forwarded to ExecOnWorkerIn")
	}
	return []byte("x86_64\n"), nil, 0, 0, 0, nil
}

func TestListWorkersRPCForwardsSelectorsAndReportsFleetMetadata(t *testing.T) {
	legacy := &mockctl.Backend{}
	legacy.SetPoolSnapshot(func() []control.WorkerView {
		return []control.WorkerView{{InstanceID: "legacy-unfiltered"}}
	})
	backend := &selectorBackend{Backend: legacy}

	_, client := newTestServer(t, backend)
	resp, err := client.ListWorkers(t.Context(), connect.NewRequest(&controlv1.ListWorkersRequest{
		Tier: selectedTier, Provider: selectedProvider,
	}))
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(resp.Msg.Workers) != 1 {
		t.Fatalf("workers length = %d, want 1", len(resp.Msg.Workers))
	}
	worker := resp.Msg.Workers[0]
	if worker.InstanceId != selectedInstance || worker.Tier != selectedTier ||
		worker.Provider != selectedProvider || worker.Driver != selectedDriver {
		t.Fatalf("worker metadata = %+v", worker)
	}
	if legacy.PoolSnapshotCalls() != 1 {
		t.Fatalf("legacy PoolSnapshot calls = %d, want 1 initial snapshot", legacy.PoolSnapshotCalls())
	}
}

func TestWriteRPCsForwardTierSelector(t *testing.T) {
	legacy := &mockctl.Backend{}
	legacy.SetForceProvision(func(context.Context) (string, error) {
		return "", errors.New("legacy ForceProvision called")
	})
	legacy.SetForceReap(func(context.Context, string) error {
		return errors.New("legacy ForceReap called")
	})
	legacy.SetExecOnWorker(func(context.Context, string, string) ([]byte, []byte, int32, int64, int64, error) {
		return nil, nil, 0, 0, 0, errors.New("legacy ExecOnWorker called")
	})
	backend := &selectorBackend{Backend: legacy}

	_, client := newWritesServer(t, backend, true)
	provision, err := client.ForceProvision(t.Context(), connect.NewRequest(&controlv1.ForceProvisionRequest{
		Tier: selectedTier,
	}))
	if err != nil {
		t.Fatalf("ForceProvision: %v", err)
	}
	if provision.Msg.InstanceId != selectedInstance {
		t.Fatalf("ForceProvision instance = %q, want long-vm", provision.Msg.InstanceId)
	}

	_, err = client.ForceReap(t.Context(), connect.NewRequest(&controlv1.ForceReapRequest{
		Tier: selectedTier, InstanceId: selectedInstance,
	}))
	if err != nil {
		t.Fatalf("ForceReap: %v", err)
	}
	if backend.reapCalls.Load() != 1 {
		t.Fatalf("ForceReapIn calls = %d, want 1", backend.reapCalls.Load())
	}

	execResult, err := client.ExecOnWorker(t.Context(), connect.NewRequest(&controlv1.ExecOnWorkerRequest{
		Tier: selectedTier, InstanceId: selectedInstance, Command: "uname -m",
	}))
	if err != nil {
		t.Fatalf("ExecOnWorker: %v", err)
	}
	if string(execResult.Msg.Stdout) != "x86_64\n" {
		t.Fatalf("ExecOnWorker stdout = %q, want x86_64", execResult.Msg.Stdout)
	}
	if legacy.ForceProvisionCalls() != 0 || legacy.ForceReapCalls() != 0 || legacy.ExecOnWorkerCalls() != 0 {
		t.Fatalf("legacy calls = provision:%d reap:%d exec:%d, want all zero",
			legacy.ForceProvisionCalls(), legacy.ForceReapCalls(), legacy.ExecOnWorkerCalls())
	}
}

var _ control.Backend = (*selectorBackend)(nil)
