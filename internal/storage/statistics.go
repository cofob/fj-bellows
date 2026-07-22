package storage

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"
)

type durationAccumulator struct {
	values []time.Duration
}

type statisticsAccumulator struct {
	group       StatisticsGroup
	queue       durationAccumulator
	dispatch    durationAccumulator
	run         durationAccumulator
	directCosts map[string]*CostTotal
	jobIDs      []int64
}

type costRow struct {
	entry        CostEntry
	tier         string
	provider     string
	repository   string
	workflow     string
	workflowFile string
	jobName      string
}

type phaseRow struct {
	kind      PhaseKind
	tier      string
	provider  string
	startedAt time.Time
	endedAt   time.Time
}

// Statistics computes exact job timing percentiles and fixed-point cost
// aggregates. Costs of different currencies are never combined.
//
//nolint:funlen,gocyclo // Aggregation keeps related outcomes, coverage, and fleet totals in one pass.
func (s *SQLite) Statistics(ctx context.Context, filter StatisticsFilter) (Statistics, error) {
	if filter.GroupBy == "" {
		filter.GroupBy = GroupNone
	}
	switch filter.GroupBy {
	case GroupNone, GroupWorkflow, GroupTier, GroupProvider, GroupDay:
	default:
		return Statistics{}, fmt.Errorf("storage: unsupported statistics grouping %q", filter.GroupBy)
	}

	jobs, err := s.statisticsJobs(ctx, filter)
	if err != nil {
		return Statistics{}, err
	}
	groups := make(map[string]*statisticsAccumulator)
	jobGroup := make(map[int64]*statisticsAccumulator, len(jobs))
	for _, job := range jobs {
		key := statisticsKey(job, filter.GroupBy)
		encoded := encodeStatisticsKey(key)
		accumulator := groups[encoded]
		if accumulator == nil {
			accumulator = &statisticsAccumulator{
				group:       StatisticsGroup{Key: key},
				directCosts: make(map[string]*CostTotal),
			}
			groups[encoded] = accumulator
		}
		accumulateJob(accumulator, job)
		accumulator.jobIDs = append(accumulator.jobIDs, job.ID)
		jobGroup[job.ID] = accumulator
	}

	historicalCosts, err := s.statisticsCosts(ctx, filter)
	if err != nil {
		return Statistics{}, err
	}
	now := time.Now().UTC()
	activeResourceCosts, err := s.activeResourceCosts(ctx, filter, now)
	if err != nil {
		return Statistics{}, err
	}
	activeSnapshotCosts, err := s.activeSnapshotCosts(ctx, filter, now)
	if err != nil {
		return Statistics{}, err
	}
	type coverage struct {
		known   bool
		unknown bool
	}
	coverageByJob := make(map[int64]coverage, len(jobs))
	// Direct job costs are aggregated from one clipped row per immutable ledger
	// entry. Fleet costs below are additionally split at UTC day boundaries;
	// keeping those paths separate preserves CostTotal.Entries as a count of
	// ledger entries even when one job happens to cross midnight.
	for _, row := range historicalCosts {
		entry := row.entry
		if entry.JobID == 0 || entry.Kind != CostDirectCompute {
			continue
		}
		clipped, ok := clipCostRow(row, filter)
		if !ok {
			continue
		}
		if accumulator := jobGroup[entry.JobID]; accumulator != nil {
			accumulateCost(accumulator.directCosts, clipped.entry)
			current := coverageByJob[entry.JobID]
			if entry.Known {
				current.known = true
			} else {
				current.unknown = true
			}
			coverageByJob[entry.JobID] = current
		}
	}

	allCosts := make([]costRow, 0, len(historicalCosts)+len(activeResourceCosts)+len(activeSnapshotCosts))
	allCosts = append(allCosts, historicalCosts...)
	allCosts = append(allCosts, activeResourceCosts...)
	allCosts = append(allCosts, activeSnapshotCosts...)
	costs := splitCostRowsByUTCDay(allCosts, filter)
	fleet := make(map[string]*FleetCostTotal)
	for _, row := range costs {
		entry := row.entry
		day := entry.StartedAt.UTC().Format("2006-01-02")
		key := strings.Join([]string{row.tier, row.provider, day, string(entry.Kind), entry.Currency}, "\x00")
		total := fleet[key]
		if total == nil {
			total = &FleetCostTotal{
				Tier:     row.tier,
				Provider: row.provider,
				Day:      day,
				Kind:     entry.Kind,
				Currency: entry.Currency,
			}
			fleet[key] = total
		}
		total.Entries++
		if entry.Known {
			total.Nanos += entry.Nanos
		} else {
			total.UnknownEntries++
		}
		total.Estimated = total.Estimated || entry.Estimated
	}
	phases, err := s.statisticsPhases(ctx, filter, now)
	if err != nil {
		return Statistics{}, err
	}
	fleetTimings := make(map[string]*durationAccumulator)
	for _, phase := range phases {
		day := phase.startedAt.UTC().Format("2006-01-02")
		key := strings.Join([]string{phase.tier, phase.provider, day, string(phase.kind)}, "\x00")
		accumulator := fleetTimings[key]
		if accumulator == nil {
			accumulator = &durationAccumulator{}
			fleetTimings[key] = accumulator
		}
		accumulator.add(interval(phase.startedAt, phase.endedAt))
	}
	routing, err := s.RoutingEffectiveness(ctx, RoutingEffectivenessFilter{
		From: filter.From, To: filter.To, Route: filter.Route, Tier: filter.Tier,
		Provider: filter.Provider, Repository: filter.Repository, Workflow: filter.Workflow,
	})
	if err != nil {
		return Statistics{}, err
	}

	result := Statistics{
		Groups:       make([]StatisticsGroup, 0, len(groups)),
		FleetCosts:   make([]FleetCostTotal, 0, len(fleet)),
		FleetTimings: make([]FleetTimingTotal, 0, len(fleetTimings)),
		Routing:      routing,
	}
	for _, accumulator := range groups {
		for _, jobID := range accumulator.jobIDs {
			coverage := coverageByJob[jobID]
			if coverage.known && !coverage.unknown {
				accumulator.group.PricedJobs++
			} else {
				accumulator.group.UnpricedJobs++
			}
		}
		accumulator.group.QueueDuration = summarizeDurations(accumulator.queue.values)
		accumulator.group.DispatchDuration = summarizeDurations(accumulator.dispatch.values)
		accumulator.group.RunDuration = summarizeDurations(accumulator.run.values)
		accumulator.group.DirectCosts = sortedCostTotals(accumulator.directCosts)
		result.Groups = append(result.Groups, accumulator.group)
	}
	for _, total := range fleet {
		result.FleetCosts = append(result.FleetCosts, *total)
	}
	for encoded, accumulator := range fleetTimings {
		parts := strings.Split(encoded, "\x00")
		result.FleetTimings = append(result.FleetTimings, FleetTimingTotal{
			Tier: parts[0], Provider: parts[1], Day: parts[2], Kind: PhaseKind(parts[3]),
			Duration: summarizeDurations(accumulator.values),
		})
	}
	sort.Slice(result.Groups, func(i, j int) bool {
		return encodeStatisticsKey(result.Groups[i].Key) < encodeStatisticsKey(result.Groups[j].Key)
	})
	sort.Slice(result.FleetCosts, func(i, j int) bool {
		a, b := result.FleetCosts[i], result.FleetCosts[j]
		left := strings.Join([]string{a.Day, a.Provider, a.Tier, string(a.Kind), a.Currency}, "\x00")
		right := strings.Join([]string{b.Day, b.Provider, b.Tier, string(b.Kind), b.Currency}, "\x00")
		return left < right
	})
	sort.Slice(result.FleetTimings, func(i, j int) bool {
		a, b := result.FleetTimings[i], result.FleetTimings[j]
		left := strings.Join([]string{a.Day, a.Provider, a.Tier, string(a.Kind)}, "\x00")
		right := strings.Join([]string{b.Day, b.Provider, b.Tier, string(b.Kind)}, "\x00")
		return left < right
	})
	return result, nil
}

func (s *SQLite) statisticsJobs(ctx context.Context, filter StatisticsFilter) ([]Job, error) {
	where, args := jobWhere(filter.From, filter.To, filter.Tier, filter.Provider,
		filter.Repository, filter.Workflow, "")
	// Queue polling may observe a job this deployment never claims. Keep that
	// audit record in history without treating it as a served workflow run.
	where = append(where, `(status <> 'observed' OR dispatched_at_ns IS NOT NULL
OR runner_started_at_ns IS NOT NULL OR completed_at_ns IS NOT NULL)`)
	if filter.Route != "" {
		where = append(where, "EXISTS (SELECT 1 FROM routing_decisions d WHERE d.job_id = jobs.id AND d.route = ?)")
		args = append(args, filter.Route)
	}
	query := "SELECT " + jobColumns + " FROM jobs"
	if len(where) != 0 {
		// where contains only package-owned clauses; values remain bound parameters.
		//nolint:gosec // No caller-controlled text is concatenated into SQL.
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *SQLite) statisticsCosts(ctx context.Context, filter StatisticsFilter) ([]costRow, error) {
	query := `SELECT c.id, c.resource_id, c.snapshot_id, c.job_id, c.price_quote_id,
c.kind, c.currency, c.nanos, c.known, c.estimated, c.started_at_ns,
c.ended_at_ns, c.recorded_at_ns,
COALESCE(r.tier, s.tier, j.tier, ''),
COALESCE(r.provider, s.provider, j.provider, ''),
COALESCE(j.repository, ''), COALESCE(j.workflow, ''),
COALESCE(j.workflow_file, ''), COALESCE(j.job_name, '')
FROM cost_entries c
LEFT JOIN resources r ON r.id = c.resource_id
LEFT JOIN snapshots s ON s.id = c.snapshot_id
LEFT JOIN jobs j ON j.id = c.job_id`
	var where []string
	var args []any
	if !filter.From.IsZero() {
		// Select intervals that may overlap the inclusive lower bound. The
		// precise clipping (including zero-duration entries) happens in Go so
		// fixed-point costs can be apportioned without floating-point loss.
		where = append(where, "c.ended_at_ns >= ?")
		args = append(args, filter.From.UTC().UnixNano())
	}
	if !filter.To.IsZero() {
		where = append(where, "c.started_at_ns < ?")
		args = append(args, filter.To.UTC().UnixNano())
	}
	if filter.Tier != "" {
		where = append(where, "COALESCE(r.tier, s.tier, j.tier, '') = ?")
		args = append(args, filter.Tier)
	}
	if filter.Provider != "" {
		where = append(where, "COALESCE(r.provider, s.provider, j.provider, '') = ?")
		args = append(args, filter.Provider)
	}
	if filter.Repository != "" {
		where = append(where, "j.repository = ?")
		args = append(args, filter.Repository)
	}
	if filter.Workflow != "" {
		where = append(where, "(j.workflow = ? OR j.workflow_file = ? OR (j.workflow_file = '' AND j.job_name = ?))")
		args = append(args, filter.Workflow, filter.Workflow, filter.Workflow)
	}
	if filter.Route != "" {
		where = append(where, "EXISTS (SELECT 1 FROM routing_decisions d WHERE d.job_id = c.job_id AND d.route = ?)")
		args = append(args, filter.Route)
	}
	if len(where) != 0 {
		// where contains only package-owned clauses; values remain bound parameters.
		//nolint:gosec // No caller-controlled text is concatenated into SQL.
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY c.id"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var costs []costRow
	for rows.Next() {
		var row costRow
		var resourceID, snapshotID, jobID, quoteID, nanos sql.NullInt64
		var known, estimated int
		var startedAt, endedAt, recordedAt int64
		if err := rows.Scan(&row.entry.ID, &resourceID, &snapshotID, &jobID,
			&quoteID, &row.entry.Kind, &row.entry.Currency, &nanos, &known,
			&estimated, &startedAt, &endedAt, &recordedAt, &row.tier,
			&row.provider, &row.repository, &row.workflow, &row.workflowFile,
			&row.jobName); err != nil {
			return nil, err
		}
		row.entry.ResourceID = resourceID.Int64
		row.entry.SnapshotID = snapshotID.Int64
		row.entry.JobID = jobID.Int64
		row.entry.PriceQuoteID = quoteID.Int64
		row.entry.Nanos = nanos.Int64
		row.entry.Known = known != 0
		row.entry.Estimated = estimated != 0
		row.entry.StartedAt = time.Unix(0, startedAt).UTC()
		row.entry.EndedAt = time.Unix(0, endedAt).UTC()
		row.entry.RecordedAt = time.Unix(0, recordedAt).UTC()
		costs = append(costs, row)
	}
	return costs, rows.Err()
}

// activeResourceCosts projects billed compute for provider allocations that
// have a durable provider creation timestamp but no terminal billed-compute
// entry yet. The projection is read-only: closing the resource still seals the
// authoritative ledger entry through the orchestrator's normal path.
//
// Provider rounding and minimums apply once to the complete allocation. Monthly
// caps then apply independently to every UTC calendar billing month touched by
// that rounded interval. The virtual row retains the allocation's real interval
// so reporting it has the same clipping and UTC-day semantics as the immutable
// row written when the resource closes.
//
//nolint:gocyclo // Projection combines dynamic filters, nullable quote recovery, and billing floors.
func (s *SQLite) activeResourceCosts(
	ctx context.Context,
	filter StatisticsFilter,
	now time.Time,
) ([]costRow, error) {
	if filter.Repository != "" || filter.Workflow != "" || filter.Route != "" {
		return nil, nil
	}
	query := `SELECT r.id, r.price_quote_id, r.tier, r.provider,
r.provider_created_at_ns, pq.currency, pq.per_hour_nanos, pq.per_month_nanos,
pq.minimum_charge_nanos, pq.billing_quantum_ns, pq.minimum_duration_ns
FROM resources r
LEFT JOIN price_quotes pq ON pq.id = r.price_quote_id
WHERE r.closed_at_ns IS NULL AND r.provider_created_at_ns IS NOT NULL
AND r.state NOT IN (?, ?, ?)
AND NOT EXISTS (
    SELECT 1 FROM cost_entries c
    WHERE c.resource_id = r.id AND c.kind = ?
)`
	args := []any{ResourceClosed, ResourceFailed, ResourceVanished, CostBilledCompute}
	if filter.Tier != "" {
		query += " AND r.tier = ?"
		args = append(args, filter.Tier)
	}
	if filter.Provider != "" {
		query += " AND r.provider = ?"
		args = append(args, filter.Provider)
	}
	query += " ORDER BY r.id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	projectionEnd := now.UTC()
	if !filter.To.IsZero() && filter.To.UTC().Before(projectionEnd) {
		projectionEnd = filter.To.UTC()
	}
	var costs []costRow
	for rows.Next() {
		var resourceID, createdAt int64
		var quoteID, perHour, perMonth, minimumCharge sql.NullInt64
		var billingQuantum, minimumDuration sql.NullInt64
		var tier, providerName string
		var currency sql.NullString
		if err := rows.Scan(&resourceID, &quoteID, &tier, &providerName, &createdAt,
			&currency, &perHour, &perMonth, &minimumCharge, &billingQuantum,
			&minimumDuration); err != nil {
			return nil, err
		}
		start := time.Unix(0, createdAt).UTC()
		if !projectionEnd.After(start) {
			continue
		}
		known := quoteID.Valid && currency.Valid && perHour.Valid && perMonth.Valid &&
			minimumCharge.Valid && billingQuantum.Valid && minimumDuration.Valid
		base := CostEntry{
			ResourceID: resourceID, PriceQuoteID: quoteID.Int64,
			Kind: CostBilledCompute, Currency: currency.String,
			Known: known, Estimated: true, RecordedAt: now.UTC(),
		}
		if !known {
			base.Currency = ""
			base.StartedAt, base.EndedAt = start, projectionEnd
			costs = append(costs, costRow{entry: base, tier: tier, provider: providerName})
			continue
		}
		quote := PriceQuote{
			Currency: currency.String, PerHourNanos: perHour.Int64,
			PerMonthNanos: perMonth.Int64, MinimumChargeNanos: minimumCharge.Int64,
			BillingQuantum:  time.Duration(billingQuantum.Int64),
			MinimumDuration: time.Duration(minimumDuration.Int64),
		}
		for _, segment := range projectedBillingSegments(quote, start, projectionEnd) {
			entry := base
			entry.StartedAt, entry.EndedAt, entry.Nanos = segment.start, segment.end, segment.nanos
			costs = append(costs, costRow{entry: entry, tier: tier, provider: providerName})
		}
	}
	return costs, rows.Err()
}

type billingSegment struct {
	start time.Time
	end   time.Time
	nanos int64
}

func projectedBillingSegments(quote PriceQuote, start, end time.Time) []billingSegment {
	if !end.After(start) {
		return nil
	}
	start, end = start.UTC(), end.UTC()
	billed := roundUpDuration(end.Sub(start), quote.BillingQuantum)
	billed = max(billed, quote.MinimumDuration)
	nanos, months := projectedMonthlyCappedNanos(
		quote.PerHourNanos,
		quote.PerMonthNanos,
		start,
		start.Add(billed),
	)
	if nanos < quote.MinimumChargeNanos {
		nanos = quote.MinimumChargeNanos
	}
	if quote.PerMonthNanos > 0 {
		if months == 0 {
			months = 1
		}
		maximum := saturatingProduct(quote.PerMonthNanos, months)
		if nanos > maximum {
			nanos = maximum
		}
	}

	// A terminal billed-compute entry also uses the actual resource interval,
	// even when provider rounding charges beyond it. Keeping that shape here
	// makes a live report stable when the projection is later sealed in history.
	return []billingSegment{{start: start, end: end, nanos: nanos}}
}

func projectedMonthlyCappedNanos(rate, monthlyCap int64, start, end time.Time) (int64, int64) {
	if rate <= 0 || !end.After(start) {
		return 0, 0
	}
	if monthlyCap <= 0 {
		return rateNanos(rate, end.Sub(start), time.Hour), 0
	}
	start, end = start.UTC(), end.UTC()
	var total, months int64
	for cursor := start; cursor.Before(end); {
		boundary := time.Date(cursor.Year(), cursor.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		segmentEnd := minTime(end, boundary)
		segment := min(rateNanos(rate, segmentEnd.Sub(cursor), time.Hour), monthlyCap)
		total = saturatingAdd(total, segment)
		months++
		cursor = segmentEnd
	}
	return total, months
}

func roundUpDuration(elapsed, quantum time.Duration) time.Duration {
	if elapsed <= 0 || quantum <= 0 || elapsed%quantum == 0 {
		return max(elapsed, 0)
	}
	return elapsed + quantum - elapsed%quantum
}

func rateNanos(rate int64, elapsed, period time.Duration) int64 {
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

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func saturatingProduct(value, count int64) int64 {
	if value <= 0 || count <= 0 {
		return 0
	}
	if value > math.MaxInt64/count {
		return math.MaxInt64
	}
	return value * count
}

func clipCostRow(row costRow, filter StatisticsFilter) (costRow, bool) {
	start, end, ok := clipReportInterval(row.entry.StartedAt, row.entry.EndedAt, filter)
	if !ok {
		return costRow{}, false
	}
	clipped := row
	clipped.entry.StartedAt, clipped.entry.EndedAt = start, end
	if clipped.entry.Known && row.entry.EndedAt.After(row.entry.StartedAt) {
		clipped.entry.Nanos = apportionedNanos(row.entry.Nanos, row.entry.StartedAt,
			row.entry.EndedAt, start, end)
	}
	return clipped, true
}

func splitCostRowsByUTCDay(rows []costRow, filter StatisticsFilter) []costRow {
	var split []costRow
	for _, row := range rows {
		start, end, ok := clipReportInterval(row.entry.StartedAt, row.entry.EndedAt, filter)
		if !ok {
			continue
		}
		if end.Equal(start) {
			point := row
			point.entry.StartedAt, point.entry.EndedAt = start, end
			split = append(split, point)
			continue
		}
		for cursor := start; cursor.Before(end); {
			nextDay := time.Date(cursor.Year(), cursor.Month(), cursor.Day()+1, 0, 0, 0, 0, time.UTC)
			segmentEnd := minTime(end, nextDay)
			segment := row
			segment.entry.StartedAt, segment.entry.EndedAt = cursor, segmentEnd
			if segment.entry.Known {
				segment.entry.Nanos = apportionedNanos(row.entry.Nanos, row.entry.StartedAt,
					row.entry.EndedAt, cursor, segmentEnd)
			}
			split = append(split, segment)
			cursor = segmentEnd
		}
	}
	return split
}

func clipReportInterval(start, end time.Time, filter StatisticsFilter) (time.Time, time.Time, bool) {
	start, end = start.UTC(), end.UTC()
	if end.Before(start) {
		return time.Time{}, time.Time{}, false
	}
	if end.Equal(start) {
		if !filter.From.IsZero() && start.Before(filter.From.UTC()) {
			return time.Time{}, time.Time{}, false
		}
		if !filter.To.IsZero() && !start.Before(filter.To.UTC()) {
			return time.Time{}, time.Time{}, false
		}
		return start, end, true
	}
	if !filter.From.IsZero() && start.Before(filter.From.UTC()) {
		start = filter.From.UTC()
	}
	if !filter.To.IsZero() && filter.To.UTC().Before(end) {
		end = filter.To.UTC()
	}
	return start, end, end.After(start)
}

func apportionedNanos(total int64, fullStart, fullEnd, segmentStart, segmentEnd time.Time) int64 {
	if total <= 0 || !fullEnd.After(fullStart) || !segmentEnd.After(segmentStart) {
		return 0
	}
	fullDuration := fullEnd.Sub(fullStart)
	startOffset := segmentStart.Sub(fullStart)
	endOffset := segmentEnd.Sub(fullStart)
	portionAt := func(offset time.Duration) *big.Int {
		value := new(big.Int).Mul(big.NewInt(total), big.NewInt(int64(offset)))
		return value.Quo(value, big.NewInt(int64(fullDuration)))
	}
	value := new(big.Int).Sub(portionAt(endOffset), portionAt(startOffset))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// activeSnapshotCosts adds a read-time estimate for provider images that are
// still accruing storage charges. Deleted snapshots have an immutable cost
// entry and are therefore excluded. The returned virtual rows retain their
// complete interval through the report endpoint; the shared splitting pass
// clips and apportions them across UTC days without mutating the ledger.
func (s *SQLite) activeSnapshotCosts(
	ctx context.Context,
	filter StatisticsFilter,
	now time.Time,
) ([]costRow, error) {
	if filter.Repository != "" || filter.Workflow != "" || filter.Route != "" {
		return nil, nil
	}
	query := `SELECT s.id, s.provider, s.tier, s.size_bytes,
COALESCE(s.completed_at_ns, s.created_at_ns),
pq.id, pq.currency, pq.snapshot_gb_month_nanos
FROM snapshots s
LEFT JOIN price_quotes pq ON pq.id = COALESCE(
    (SELECT r.price_quote_id FROM resources r WHERE r.id = s.source_resource_id),
    (SELECT q.id FROM price_quotes q WHERE q.provider = s.provider
     ORDER BY q.observed_at_ns DESC, q.id DESC LIMIT 1))
WHERE s.state IN (?, ?, ?)`
	args := []any{SnapshotActive, SnapshotStale, SnapshotDeleting}
	if filter.Tier != "" {
		query += " AND s.tier = ?"
		args = append(args, filter.Tier)
	}
	if filter.Provider != "" {
		query += " AND s.provider = ?"
		args = append(args, filter.Provider)
	}
	query += " ORDER BY s.id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var costs []costRow
	for rows.Next() {
		var snapshotID, sizeBytes, startedAt int64
		var providerName, tier string
		var quoteID, snapshotRate sql.NullInt64
		var currency sql.NullString
		if err := rows.Scan(&snapshotID, &providerName, &tier, &sizeBytes, &startedAt,
			&quoteID, &currency, &snapshotRate); err != nil {
			return nil, err
		}
		start := time.Unix(0, startedAt).UTC()
		end := now
		if !filter.To.IsZero() && filter.To.Before(end) {
			end = filter.To.UTC()
		}
		if !end.After(start) {
			continue
		}
		entry := CostEntry{
			SnapshotID: snapshotID, PriceQuoteID: quoteID.Int64,
			Kind: CostSnapshotStorage, Currency: currency.String,
			Known:     quoteID.Valid && snapshotRate.Valid && sizeBytes > 0,
			Estimated: true, StartedAt: start, EndedAt: end, RecordedAt: now,
		}
		if entry.Known {
			entry.Nanos = snapshotStorageNanos(snapshotRate.Int64, sizeBytes, end.Sub(start))
		} else {
			entry.Currency = ""
		}
		costs = append(costs, costRow{entry: entry, tier: tier, provider: providerName})
	}
	return costs, rows.Err()
}

func snapshotStorageNanos(gbMonthRate, sizeBytes int64, elapsed time.Duration) int64 {
	if gbMonthRate < 0 || sizeBytes <= 0 || elapsed <= 0 {
		return 0
	}
	const (
		bytesPerGB = int64(1_000_000_000)
		month      = 30 * 24 * time.Hour
	)
	value := new(big.Int).Mul(big.NewInt(gbMonthRate), big.NewInt(sizeBytes))
	value.Mul(value, big.NewInt(int64(elapsed)))
	value.Quo(value, big.NewInt(bytesPerGB))
	value.Quo(value, big.NewInt(int64(month)))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}

//nolint:gocyclo // Optional report dimensions map directly to independent bound SQL predicates.
func (s *SQLite) statisticsPhases(
	ctx context.Context,
	filter StatisticsFilter,
	now time.Time,
) ([]phaseRow, error) {
	query := `SELECT p.kind, COALESCE(r.tier, j.tier, ''),
	COALESCE(r.provider, j.provider, ''), p.started_at_ns, p.ended_at_ns
	FROM phases p
	JOIN resources r ON r.id = p.resource_id
	LEFT JOIN jobs j ON j.id = p.job_id`
	var where []string
	var args []any
	if !filter.From.IsZero() {
		where = append(where, "(p.ended_at_ns IS NULL OR p.ended_at_ns >= ?)")
		args = append(args, filter.From.UTC().UnixNano())
	}
	if !filter.To.IsZero() {
		where = append(where, "p.started_at_ns < ?")
		args = append(args, filter.To.UTC().UnixNano())
	}
	if filter.Tier != "" {
		where = append(where, "COALESCE(r.tier, j.tier, '') = ?")
		args = append(args, filter.Tier)
	}
	if filter.Provider != "" {
		where = append(where, "COALESCE(r.provider, j.provider, '') = ?")
		args = append(args, filter.Provider)
	}
	if filter.Repository != "" {
		where = append(where, "j.repository = ?")
		args = append(args, filter.Repository)
	}
	if filter.Workflow != "" {
		where = append(where, "(j.workflow = ? OR j.workflow_file = ? OR (j.workflow_file = '' AND j.job_name = ?))")
		args = append(args, filter.Workflow, filter.Workflow, filter.Workflow)
	}
	if filter.Route != "" {
		where = append(where, "EXISTS (SELECT 1 FROM routing_decisions d WHERE d.job_id = p.job_id AND d.route = ?)")
		args = append(args, filter.Route)
	}
	if len(where) != 0 {
		// where contains only package-owned clauses; values remain bound parameters.
		//nolint:gosec // No caller-controlled text is concatenated into SQL.
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY p.id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var phases []phaseRow
	for rows.Next() {
		var phase phaseRow
		var startedAt int64
		var endedAt sql.NullInt64
		if err := rows.Scan(&phase.kind, &phase.tier, &phase.provider,
			&startedAt, &endedAt); err != nil {
			return nil, err
		}
		phase.startedAt = time.Unix(0, startedAt).UTC()
		if endedAt.Valid {
			phase.endedAt = time.Unix(0, endedAt.Int64).UTC()
		} else {
			phase.endedAt = now.UTC()
			if !filter.To.IsZero() && filter.To.UTC().Before(phase.endedAt) {
				phase.endedAt = filter.To.UTC()
			}
		}
		phases = append(phases, phase)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return splitPhaseRowsByUTCDay(phases, filter), nil
}

func splitPhaseRowsByUTCDay(phases []phaseRow, filter StatisticsFilter) []phaseRow {
	var split []phaseRow
	for _, phase := range phases {
		start, end, ok := clipReportInterval(phase.startedAt, phase.endedAt, filter)
		if !ok {
			continue
		}
		if end.Equal(start) {
			phase.startedAt, phase.endedAt = start, end
			split = append(split, phase)
			continue
		}
		for cursor := start; cursor.Before(end); {
			nextDay := time.Date(cursor.Year(), cursor.Month(), cursor.Day()+1, 0, 0, 0, 0, time.UTC)
			segmentEnd := minTime(end, nextDay)
			segment := phase
			segment.startedAt, segment.endedAt = cursor, segmentEnd
			split = append(split, segment)
			cursor = segmentEnd
		}
	}
	return split
}

func statisticsKey(job Job, groupBy StatisticsGroupBy) StatisticsKey {
	switch groupBy {
	case GroupWorkflow:
		return StatisticsKey{
			Source:       job.Source,
			RepositoryID: job.RepositoryID,
			Repository:   job.Repository,
			Workflow:     workflowIdentity(job),
		}
	case GroupTier:
		return StatisticsKey{Tier: job.Tier}
	case GroupProvider:
		return StatisticsKey{Provider: job.Provider}
	case GroupDay:
		return StatisticsKey{Day: job.FirstSeenAt.UTC().Format("2006-01-02")}
	case GroupNone:
		return StatisticsKey{}
	}
	return StatisticsKey{}
}

func workflowIdentity(job Job) string {
	if job.WorkflowFile != "" {
		return job.WorkflowFile
	}
	if job.Workflow != "" {
		return job.Workflow
	}
	return job.JobName
}

func encodeStatisticsKey(key StatisticsKey) string {
	return strings.Join([]string{
		key.Source,
		fmt.Sprintf("%020d", key.RepositoryID),
		key.Repository,
		key.Workflow,
		key.Tier,
		key.Provider,
		key.Day,
	}, "\x00")
}

func accumulateJob(accumulator *statisticsAccumulator, job Job) {
	group := &accumulator.group
	group.Jobs++
	switch job.Status {
	case JobSucceeded:
		group.Completed++
		group.Succeeded++
	case JobFailed:
		group.Completed++
		group.Failed++
	case JobCancelled:
		group.Completed++
		group.Cancelled++
	case JobSkipped:
		group.Completed++
		group.Skipped++
	case JobInfraFailed:
		group.Completed++
		group.InfraFailed++
	case JobInterrupted:
		group.Completed++
		group.Interrupted++
	case JobObserved, JobAssigned, JobRunning:
		group.InProgress++
	}
	accumulator.queue.add(interval(job.QueuedAt, job.DispatchedAt))
	accumulator.dispatch.add(interval(job.DispatchedAt, job.RunnerStartedAt))
	accumulator.run.add(interval(job.RunnerStartedAt, job.RunnerFinishedAt))
}

func interval(start, end time.Time) (time.Duration, bool) {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0, false
	}
	return end.Sub(start), true
}

func (a *durationAccumulator) add(value time.Duration, ok bool) {
	if ok {
		a.values = append(a.values, value)
	}
}

func summarizeDurations(values []time.Duration) DurationSummary {
	if len(values) == 0 {
		return DurationSummary{}
	}
	copyOfValues := append([]time.Duration(nil), values...)
	slices.Sort(copyOfValues)
	summary := DurationSummary{
		Count: int64(len(copyOfValues)),
		Min:   copyOfValues[0],
		Max:   copyOfValues[len(copyOfValues)-1],
		P50:   nearestRank(copyOfValues, 50),
		P95:   nearestRank(copyOfValues, 95),
	}
	for _, value := range copyOfValues {
		summary.Total += value
	}
	return summary
}

func nearestRank(values []time.Duration, percentile int) time.Duration {
	index := (percentile*len(values) + 99) / 100
	index = max(index, 1)
	return values[index-1]
}

func accumulateCost(totals map[string]*CostTotal, entry CostEntry) {
	key := string(entry.Kind) + "\x00" + entry.Currency
	total := totals[key]
	if total == nil {
		total = &CostTotal{Kind: entry.Kind, Currency: entry.Currency}
		totals[key] = total
	}
	total.Entries++
	if entry.Known {
		total.Nanos += entry.Nanos
	} else {
		total.UnknownEntries++
	}
	total.Estimated = total.Estimated || entry.Estimated
}

func sortedCostTotals(totals map[string]*CostTotal) []CostTotal {
	result := make([]CostTotal, 0, len(totals))
	for _, total := range totals {
		result = append(result, *total)
	}
	sort.Slice(result, func(i, j int) bool {
		left := string(result[i].Kind) + "\x00" + result[i].Currency
		right := string(result[j].Kind) + "\x00" + result[j].Currency
		return left < right
	})
	return result
}
