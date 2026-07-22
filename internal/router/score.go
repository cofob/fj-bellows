package router

import (
	"math"
	"math/big"
	"slices"
	"time"

	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/provider"
	"github.com/hstern/fj-bellows/internal/storage"
)

const (
	defaultProvisionP95 = 2 * time.Minute
	defaultResetP95     = 2 * time.Minute
	resetSnapshot       = "snapshot"
	reasonCapacityFull  = "capacity_full"
)

type scoredCandidate struct {
	storage.RoutingCandidateScore
	startDelay time.Duration
	idleIndex  int
	laneIndex  int
}

func scoreCandidate(
	snapshot orchestrator.RoutingTierSnapshot,
	quote storage.PriceQuote,
	fxRate string,
	jobLabels []string,
	predictedRun, provisionP95, resetP95 time.Duration,
	now time.Time,
) scoredCandidate {
	return scoreCandidateWithQueue(snapshot, quote, fxRate, jobLabels, predictedRun,
		provisionP95, resetP95, now, nil, false)
}

//nolint:gocyclo // Eligibility, paid-window lanes, and cold billing must share one deterministic ranking pass.
func scoreCandidateWithQueue(
	snapshot orchestrator.RoutingTierSnapshot,
	quote storage.PriceQuote,
	fxRate string,
	jobLabels []string,
	predictedRun, provisionP95, resetP95 time.Duration,
	now time.Time,
	lanes []optimizationLane,
	allowOptimizationQueue bool,
) scoredCandidate {
	result := scoredCandidate{RoutingCandidateScore: storage.RoutingCandidateScore{
		Tier: snapshot.Tier, Provider: snapshot.ProviderName, Driver: snapshot.Driver,
		InstanceType: snapshot.InstanceType, IdleWorkers: len(snapshot.IdleWorkers),
		ActiveWorkers: snapshot.ActiveWorkers, PendingWorkers: snapshot.PendingWorkers,
		MaxInstances: snapshot.MaxInstances, PredictedRun: predictedRun,
		PriceQuoteID: quote.ID, NativeCurrency: quote.Currency, FXRate: fxRate,
	}, idleIndex: -1, laneIndex: -1}
	if provisionP95 <= 0 {
		provisionP95 = defaultProvisionP95
	}
	if resetP95 <= 0 {
		resetP95 = defaultResetP95
	}
	result.ProvisionP95 = provisionP95
	result.ResetP95 = resetP95
	if !snapshot.Healthy {
		result.Reason = "tier_unhealthy"
		return result
	}
	if !serviceable(jobLabels, snapshot.Labels) {
		result.Reason = "labels_incompatible"
		return result
	}
	queuePossible := allowOptimizationQueue && reusableForOptimization(snapshot) && hasUsableLane(lanes, now)
	if len(snapshot.IdleWorkers) == 0 && snapshot.AvailableSlots <= 0 && !queuePossible {
		result.Reason = reasonCapacityFull
		return result
	}
	result.Eligible = true
	if quote.ID == 0 || fxRate == "" {
		return scoreWithoutPricing(result, snapshot, lanes, queuePossible,
			predictedRun, provisionP95, resetP95, now)
	}
	options := make([]scoredCandidate, 0, len(snapshot.IdleWorkers)+len(lanes)+1)
	for index, worker := range snapshot.IdleWorkers {
		horizon := predictedRun
		if shouldIncludeReset(snapshot, worker, now.Add(predictedRun)) {
			horizon += resetP95
		}
		before := billedCost(quote, worker.CreatedAt, now)
		after := billedCost(quote, worker.CreatedAt, now.Add(horizon))
		marginal := max(after-before, 0)
		option := result
		setDirectIdleCandidate(&option, snapshot, worker, index, predictedRun, resetP95, now, lanes)
		setCandidateCost(&option, marginal, fxRate)
		options = append(options, option)
	}
	if queuePossible {
		for index, lane := range lanes {
			option, ok := queuedCandidate(result, snapshot, lane, index, predictedRun, resetP95, now)
			if !ok {
				continue
			}
			before := billedCost(quote, lane.worker.CreatedAt, now)
			after := billedCost(quote, lane.worker.CreatedAt, option.ScheduledFinishAt)
			setCandidateCost(&option, max(after-before, 0), fxRate)
			options = append(options, option)
		}
	}
	if snapshot.AvailableSlots > 0 {
		option := result
		option.startDelay = provisionP95
		option.ScheduledStartAt = now.Add(provisionP95)
		option.ScheduledFinishAt = option.ScheduledStartAt.Add(coldRunHorizon(snapshot, predictedRun, resetP95))
		setCandidateCost(&option, billedCost(quote, now, option.ScheduledFinishAt), fxRate)
		options = append(options, option)
	}
	if len(options) == 0 {
		result.Eligible = false
		result.Reason = reasonCapacityFull
		return result
	}
	best := options[0]
	for _, option := range options[1:] {
		if allocationLess(option, best) {
			best = option
		}
	}
	return best
}

func scoreWithoutPricing(
	result scoredCandidate,
	snapshot orchestrator.RoutingTierSnapshot,
	lanes []optimizationLane,
	queuePossible bool,
	predictedRun, provisionP95, resetP95 time.Duration,
	now time.Time,
) scoredCandidate {
	result.Reason = "pricing_unavailable"
	switch {
	case len(snapshot.IdleWorkers) > 0:
		setDirectIdleCandidate(&result, snapshot, snapshot.IdleWorkers[0], 0,
			predictedRun, resetP95, now, lanes)
	case queuePossible:
		if queued, ok := firstQueuedCandidate(result, snapshot, lanes, predictedRun, resetP95, now); ok {
			return queued
		}
		if snapshot.AvailableSlots <= 0 {
			result.Eligible = false
			result.Reason = reasonCapacityFull
			return result
		}
		setColdSchedule(&result, snapshot, predictedRun, provisionP95, resetP95, now)
	default:
		setColdSchedule(&result, snapshot, predictedRun, provisionP95, resetP95, now)
	}
	return result
}

func setColdSchedule(
	result *scoredCandidate,
	snapshot orchestrator.RoutingTierSnapshot,
	predictedRun, provisionP95, resetP95 time.Duration,
	now time.Time,
) {
	result.startDelay = provisionP95
	result.ScheduledStartAt = now.Add(provisionP95)
	result.ScheduledFinishAt = result.ScheduledStartAt.Add(coldRunHorizon(snapshot, predictedRun, resetP95))
}

func setIdleCandidate(result *scoredCandidate, worker orchestrator.WorkerView, index int) {
	result.UsedIdle = true
	result.idleIndex = index
	result.IdleInstanceID = worker.InstanceID
	result.IdleCreatedAt = worker.CreatedAt
	result.IdlePaidHourEndAt = worker.PaidHourEndAt
	result.IdleReapEligibleAt = worker.ReapEligibleAt
}

func setDirectIdleCandidate(
	result *scoredCandidate,
	snapshot orchestrator.RoutingTierSnapshot,
	worker orchestrator.WorkerView,
	index int,
	predictedRun, resetP95 time.Duration,
	now time.Time,
	lanes []optimizationLane,
) {
	setIdleCandidate(result, worker, index)
	result.laneIndex = laneForIdleWorker(lanes, worker)
	result.ScheduledStartAt = now
	result.ScheduledFinishAt = now.Add(predictedRun)
	if shouldIncludeReset(snapshot, worker, result.ScheduledFinishAt) {
		result.ScheduledFinishAt = result.ScheduledFinishAt.Add(resetP95)
	}
}

func firstQueuedCandidate(
	base scoredCandidate,
	snapshot orchestrator.RoutingTierSnapshot,
	lanes []optimizationLane,
	predictedRun, resetP95 time.Duration,
	now time.Time,
) (scoredCandidate, bool) {
	for index, lane := range lanes {
		if option, ok := queuedCandidate(base, snapshot, lane, index, predictedRun, resetP95, now); ok {
			return option, true
		}
	}
	return scoredCandidate{}, false
}

func queuedCandidate(
	base scoredCandidate,
	snapshot orchestrator.RoutingTierSnapshot,
	lane optimizationLane,
	laneIndex int,
	predictedRun, resetP95 time.Duration,
	now time.Time,
) (scoredCandidate, bool) {
	if lane.blocked || lane.worker.ReapEligibleAt.IsZero() || !lane.worker.ReapEligibleAt.After(now) {
		return scoredCandidate{}, false
	}
	start := lane.availableAt
	if start.Before(now) {
		start = now
	}
	finish := start.Add(predictedRun)
	if snapshot.OneJobPerVM && snapshot.ResetMode == resetSnapshot {
		finish = finish.Add(resetP95)
	}
	if finish.After(lane.worker.ReapEligibleAt) {
		return scoredCandidate{}, false
	}
	result := base
	setIdleCandidate(&result, lane.worker, -1)
	result.laneIndex = laneIndex
	result.startDelay = start.Sub(now)
	result.OptimizationQueued = true
	result.OptimizationQueuePosition = lane.queued + 1
	result.OptimizationWait = result.startDelay
	result.ScheduledStartAt = start
	result.ScheduledFinishAt = finish
	return result, true
}

func hasUsableLane(lanes []optimizationLane, now time.Time) bool {
	for _, lane := range lanes {
		if !lane.blocked && lane.worker.ReapEligibleAt.After(now) {
			return true
		}
	}
	return false
}

func reusableForOptimization(snapshot orchestrator.RoutingTierSnapshot) bool {
	return snapshot.Teardown.Model == provider.BillingHourlyRoundUp &&
		(!snapshot.OneJobPerVM || snapshot.ResetMode == resetSnapshot)
}

func coldRunHorizon(snapshot orchestrator.RoutingTierSnapshot, predictedRun, resetP95 time.Duration) time.Duration {
	horizon := predictedRun
	if snapshot.OneJobPerVM && snapshot.ResetMode == resetSnapshot {
		horizon += resetP95
	}
	return horizon
}

func setCandidateCost(result *scoredCandidate, native int64, fxRate string) {
	result.NativeCostKnown = true
	result.NativeCostNanos = native
	if normalized, ok := normalizeNanos(native, fxRate); ok {
		result.ScoreCostKnown = true
		result.ScoreCostNanos = normalized
	} else {
		result.Reason = "invalid_fx_rate"
	}
}

func allocationLess(left, right scoredCandidate) bool {
	if left.NativeCostNanos != right.NativeCostNanos {
		return left.NativeCostNanos < right.NativeCostNanos
	}
	if left.UsedIdle != right.UsedIdle {
		return left.UsedIdle
	}
	if left.startDelay != right.startDelay {
		return left.startDelay < right.startDelay
	}
	return left.OptimizationQueuePosition < right.OptimizationQueuePosition
}

func shouldIncludeReset(snapshot orchestrator.RoutingTierSnapshot, worker orchestrator.WorkerView, finish time.Time) bool {
	if !snapshot.OneJobPerVM || snapshot.ResetMode != resetSnapshot {
		return false
	}
	if worker.ReapEligibleAt.IsZero() || !finish.Before(worker.ReapEligibleAt) {
		return true
	}
	return worker.ReapEligibleAt.Sub(finish) >= snapshot.ResetMinRemaining
}

func serviceable(want, offered []string) bool {
	have := forgejo.BareLabels(offered)
	for _, label := range want {
		if !slices.Contains(have, label) {
			return false
		}
	}
	return true
}

func billedCost(quote storage.PriceQuote, start, end time.Time) int64 {
	if !end.After(start) {
		return 0
	}
	elapsed := end.Sub(start)
	if quote.BillingQuantum > 0 && elapsed%quote.BillingQuantum != 0 {
		elapsed += quote.BillingQuantum - elapsed%quote.BillingQuantum
	}
	if elapsed < quote.MinimumDuration {
		elapsed = quote.MinimumDuration
	}
	total := proportional(quote.PerHourNanos, elapsed, time.Hour)
	total = max(total, quote.MinimumChargeNanos)
	if quote.PerMonthNanos <= 0 {
		return total
	}
	// Provider monthly caps are applied independently to every UTC calendar
	// month touched by the billed interval.
	billedEnd := start.Add(elapsed)
	var capped, months int64
	for cursor := start.UTC(); cursor.Before(billedEnd.UTC()); {
		boundary := time.Date(cursor.Year(), cursor.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		segmentEnd := minTime(billedEnd.UTC(), boundary)
		segment := min(proportional(quote.PerHourNanos, segmentEnd.Sub(cursor), time.Hour), quote.PerMonthNanos)
		capped = saturatingAdd(capped, segment)
		months++
		cursor = segmentEnd
	}
	if capped < quote.MinimumChargeNanos {
		capped = quote.MinimumChargeNanos
	}
	return min(capped, saturatingMultiply(quote.PerMonthNanos, months))
}

func proportional(rate int64, elapsed, period time.Duration) int64 {
	if rate <= 0 || elapsed <= 0 || period <= 0 {
		return 0
	}
	value := new(big.Int).Mul(big.NewInt(rate), big.NewInt(int64(elapsed)))
	value.Quo(value, big.NewInt(int64(period)))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}

func normalizeNanos(value int64, rate string) (int64, bool) {
	ratio, ok := new(big.Rat).SetString(rate)
	if !ok || ratio.Sign() <= 0 || value < 0 {
		return 0, false
	}
	numerator := new(big.Int).Mul(big.NewInt(value), ratio.Num())
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, ratio.Denom(), remainder)
	if new(big.Int).Lsh(remainder, 1).Cmp(ratio.Denom()) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return math.MaxInt64, true
	}
	return quotient.Int64(), true
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func saturatingMultiply(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}
