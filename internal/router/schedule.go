package router

import (
	"context"
	"time"

	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/storage"
)

type optimizationLane struct {
	worker      orchestrator.WorkerView
	availableAt time.Time
	queued      int
	blocked     bool
}

type schedulingState struct {
	lanes         map[string][]optimizationLane
	queuedByRoute map[string]int
}

//nolint:gocyclo,nestif // Replay validates interdependent worker, lease, and paid-window state in one pass.
func loadSchedulingState(
	ctx context.Context,
	store storage.RoutingStore,
	snapshots map[string]orchestrator.RoutingTierSnapshot,
	now time.Time,
) (*schedulingState, error) {
	state := &schedulingState{
		lanes:         make(map[string][]optimizationLane, len(snapshots)),
		queuedByRoute: map[string]int{},
	}
	workers := make(map[string]map[string]orchestrator.WorkerView, len(snapshots))
	for tier, snapshot := range snapshots {
		workers[tier] = map[string]orchestrator.WorkerView{}
		all := snapshot.Workers
		if len(all) == 0 {
			all = snapshot.IdleWorkers
		}
		for _, worker := range all {
			if worker.InstanceID != "" {
				workers[tier][worker.InstanceID] = worker
			}
		}
		for _, worker := range snapshot.IdleWorkers {
			state.lanes[tier] = append(state.lanes[tier], optimizationLane{
				worker: worker, availableAt: now,
			})
		}
	}

	reservations, err := store.RoutingReservations(ctx, now)
	if err != nil {
		return nil, err
	}
	blockedWorkers := map[string]bool{}
	for _, reservation := range reservations {
		worker, alive := workers[reservation.Tier][reservation.WorkerID]
		workerKey := reservation.Tier + "\x00" + reservation.WorkerID
		if alive {
			removeReservedIdle(snapshots, reservation.Tier, reservation.WorkerID)
		}
		validWindow := alive && !worker.ReapEligibleAt.IsZero() &&
			reservation.ScheduledFinishAt.After(now) &&
			!reservation.ScheduledFinishAt.After(worker.ReapEligibleAt)
		if !validWindow {
			if alive {
				blockedWorkers[workerKey] = true
				if index := findLane(state.lanes[reservation.Tier], reservation.WorkerID); index >= 0 {
					lane := state.lanes[reservation.Tier][index]
					lane.blocked = true
					state.lanes[reservation.Tier][index] = lane
				}
			}
			if reservation.OptimizationQueued && reservation.OptimizationActive &&
				reservation.Status == storage.JobObserved {
				if err := store.ReleaseRoutingOptimization(ctx, reservation.JobID, now); err != nil {
					return nil, err
				}
			}
			continue
		}
		index := findLane(state.lanes[reservation.Tier], reservation.WorkerID)
		if index < 0 {
			state.lanes[reservation.Tier] = append(state.lanes[reservation.Tier], optimizationLane{
				worker: worker, availableAt: now,
			})
			index = len(state.lanes[reservation.Tier]) - 1
		}
		lane := state.lanes[reservation.Tier][index]
		lane.blocked = lane.blocked || blockedWorkers[workerKey]
		if reservation.ScheduledFinishAt.After(lane.availableAt) {
			lane.availableAt = reservation.ScheduledFinishAt
		}
		if reservation.OptimizationQueued && reservation.OptimizationActive &&
			reservation.Status == storage.JobObserved {
			lane.queued++
			state.queuedByRoute[reservation.Route]++
		}
		if worker.State != string(orchestrator.StateIdle) &&
			!reservation.ScheduledFinishAt.After(now) {
			lane.blocked = true
		}
		state.lanes[reservation.Tier][index] = lane
	}
	return state, nil
}

func removeReservedIdle(
	snapshots map[string]orchestrator.RoutingTierSnapshot,
	tier, workerID string,
) {
	if workerID == "" {
		return
	}
	snapshot := snapshots[tier]
	for i := range snapshot.IdleWorkers {
		if snapshot.IdleWorkers[i].InstanceID != workerID {
			continue
		}
		snapshot.IdleWorkers = append(snapshot.IdleWorkers[:i], snapshot.IdleWorkers[i+1:]...)
		snapshots[tier] = snapshot
		return
	}
}

func findLane(lanes []optimizationLane, workerID string) int {
	if workerID == "" {
		return -1
	}
	for i := range lanes {
		if lanes[i].worker.InstanceID == workerID {
			return i
		}
	}
	return -1
}

func laneForIdleWorker(lanes []optimizationLane, worker orchestrator.WorkerView) int {
	if index := findLane(lanes, worker.InstanceID); index >= 0 {
		return index
	}
	for i := range lanes {
		if lanes[i].worker.InstanceID == "" && lanes[i].worker.CreatedAt.Equal(worker.CreatedAt) {
			return i
		}
	}
	return -1
}
