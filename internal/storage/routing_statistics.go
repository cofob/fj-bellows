package storage

import (
	"context"
	"database/sql"
	"math"
	"math/big"
	"sort"
	"strings"
)

type routingEffectRow struct {
	route, label, currency, profileSource string
	selectedTier, selectedProvider        string
	jobStatus                             JobStatus
	fxRate, nativeCurrency                string
	selectedIdle                          bool
	deferred                              bool
	predictedP95                          int64
	selectedCost, fallbackCost            sql.NullInt64
	runnerStarted, runnerFinished         sql.NullInt64
	actualNanos                           sql.NullInt64
	actualUnknown                         int64
}

// RoutingEffectiveness aggregates immutable routing scorecards and eventual
// job outcomes. Estimated savings always use the configured fallback tier.
func (s *SQLite) RoutingEffectiveness(ctx context.Context, filter RoutingEffectivenessFilter) ([]RoutingEffectiveness, error) {
	query := `SELECT d.route, d.required_label, d.score_currency, d.profile_source,
d.selected_tier, d.selected_provider, j.status, d.selected_idle,
CASE WHEN d.defer_count > 0 THEN 1 ELSE 0 END, d.predicted_p95_ns,
d.selected_cost_nanos, d.fallback_cost_nanos, j.runner_started_at_ns,
j.runner_finished_at_ns, c.fx_rate, c.native_currency,
(SELECT SUM(e.nanos) FROM cost_entries e WHERE e.job_id = j.id
 AND e.kind = 'direct_compute' AND e.known = 1 AND e.currency = c.native_currency),
(SELECT COUNT(*) FROM cost_entries e WHERE e.job_id = j.id
 AND e.kind = 'direct_compute' AND (e.known = 0 OR e.currency <> c.native_currency))
FROM routing_decisions d
JOIN jobs j ON j.id = d.job_id
JOIN routing_candidate_scores c ON c.decision_id = d.id AND c.selected = 1`
	where := []string{"d.state = ?", "d.decided_at_ns IS NOT NULL"}
	args := []any{RoutingAssigned}
	if !filter.From.IsZero() {
		where = append(where, "d.decided_at_ns >= ?")
		args = append(args, filter.From.UnixNano())
	}
	if !filter.To.IsZero() {
		where = append(where, "d.decided_at_ns < ?")
		args = append(args, filter.To.UnixNano())
	}
	filters := []struct{ clause, value string }{
		{clause: "d.route = ?", value: filter.Route},
		{clause: "d.selected_tier = ?", value: filter.Tier},
		{clause: "d.selected_provider = ?", value: filter.Provider},
		{clause: "j.repository = ?", value: filter.Repository},
	}
	for _, item := range filters {
		clause, value := item.clause, item.value
		if value != "" {
			where = append(where, clause)
			args = append(args, value)
		}
	}
	if filter.Workflow != "" {
		where = append(where, "COALESCE(NULLIF(j.workflow_file, ''), NULLIF(j.workflow, ''), j.job_name) = ?")
		args = append(args, filter.Workflow)
	}
	// where contains only package-owned clauses; values remain bound parameters.
	//nolint:gosec // No caller-controlled text is concatenated into SQL.
	query += " WHERE " + strings.Join(where, " AND ")
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	groups := map[string]*RoutingEffectiveness{}
	selections := map[string]map[string]*RoutingSelection{}
	for rows.Next() {
		row, err := scanRoutingEffectRow(rows)
		if err != nil {
			return nil, err
		}
		key := row.route + "\x00" + row.currency
		group := groups[key]
		if group == nil {
			group = &RoutingEffectiveness{Route: row.route, RequiredLabel: row.label, Currency: row.currency}
			groups[key] = group
			selections[key] = map[string]*RoutingSelection{}
		}
		accumulateRoutingEffect(group, selections[key], row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]RoutingEffectiveness, 0, len(groups))
	for key, group := range groups {
		for _, selection := range selections[key] {
			group.Selections = append(group.Selections, *selection)
		}
		sort.Slice(group.Selections, func(i, j int) bool {
			if group.Selections[i].Tier == group.Selections[j].Tier {
				return group.Selections[i].Provider < group.Selections[j].Provider
			}
			return group.Selections[i].Tier < group.Selections[j].Tier
		})
		result = append(result, *group)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Route+"\x00"+result[i].Currency < result[j].Route+"\x00"+result[j].Currency
	})
	return result, nil
}

func scanRoutingEffectRow(scanner interface{ Scan(...any) error }) (routingEffectRow, error) {
	var row routingEffectRow
	var selectedIdle, deferred int
	err := scanner.Scan(&row.route, &row.label, &row.currency, &row.profileSource,
		&row.selectedTier, &row.selectedProvider, &row.jobStatus, &selectedIdle,
		&deferred, &row.predictedP95, &row.selectedCost, &row.fallbackCost,
		&row.runnerStarted, &row.runnerFinished, &row.fxRate, &row.nativeCurrency,
		&row.actualNanos, &row.actualUnknown)
	row.selectedIdle = selectedIdle == 1
	row.deferred = deferred == 1
	return row, err
}

//nolint:gocyclo // One row updates the complete, deliberately denormalized effectiveness aggregate.
func accumulateRoutingEffect(group *RoutingEffectiveness, selections map[string]*RoutingSelection, row routingEffectRow) {
	group.Decisions++
	if row.selectedIdle {
		group.IdleDecisions++
	}
	if row.deferred {
		group.DeferredDecisions++
	}
	if strings.HasPrefix(row.profileSource, "cold_fallback") {
		group.FallbackDecisions++
	} else {
		group.HistoryDecisions++
	}
	if row.selectedCost.Valid {
		group.EstimatedSelectedNanos = saturatedAdd(group.EstimatedSelectedNanos, row.selectedCost.Int64)
	}
	if row.fallbackCost.Valid {
		group.EstimatedFallbackNanos = saturatedAdd(group.EstimatedFallbackNanos, row.fallbackCost.Int64)
		if row.selectedCost.Valid {
			group.EstimatedSavingsNanos = saturatedAdd(group.EstimatedSavingsNanos,
				routingSavings(row.fallbackCost.Int64, row.selectedCost.Int64))
		}
	}
	if routingTerminalStatus(row.jobStatus) {
		group.Completed++
	}
	if (row.jobStatus == JobSucceeded || row.jobStatus == JobFailed) &&
		row.runnerStarted.Valid && row.runnerFinished.Valid && row.runnerFinished.Int64 >= row.runnerStarted.Int64 {
		if row.runnerFinished.Int64-row.runnerStarted.Int64 <= row.predictedP95 {
			group.P95Hits++
		} else {
			group.P95Misses++
		}
	}
	if row.actualNanos.Valid {
		if converted, ok := convertRoutingNanos(row.actualNanos.Int64, row.fxRate); ok {
			group.ActualDirectNanos = saturatedAdd(group.ActualDirectNanos, converted)
		} else {
			group.ActualUnknownEntries++
		}
	}
	group.ActualUnknownEntries += row.actualUnknown
	selectionKey := routingSelectionKey(row.selectedTier, row.selectedProvider)
	selection := selections[selectionKey]
	if selection == nil {
		selection = &RoutingSelection{Tier: row.selectedTier, Provider: row.selectedProvider}
		selections[selectionKey] = selection
	}
	selection.Jobs++
}

func routingTerminalStatus(status JobStatus) bool {
	switch status {
	case JobSucceeded, JobFailed, JobCancelled, JobSkipped, JobInfraFailed, JobInterrupted:
		return true
	case JobObserved, JobAssigned, JobRunning:
		return false
	default:
		return false
	}
}

func convertRoutingNanos(value int64, rate string) (int64, bool) {
	if value <= 0 {
		return 0, true
	}
	ratio, ok := new(big.Rat).SetString(rate)
	if !ok || ratio.Sign() <= 0 {
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

func saturatedAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}
