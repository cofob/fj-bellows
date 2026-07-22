package main

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	controlv1 "github.com/hstern/fj-bellows/gen/fjbellows/control/v1"
)

func cmdStats(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cf commonFlags
	registerCommonFlags(fs, &cf)
	since := fs.String("since", "", "only records within this duration (for example 24h)")
	from := fs.String("from", "", "inclusive RFC3339 time")
	to := fs.String("to", "", "exclusive RFC3339 time")
	tier := fs.String("tier", "", "only this tier")
	provider := fs.String("provider", "", "only this named provider")
	repository := fs.String("repository", "", "only this repository")
	workflow := fs.String("workflow", "", "only this workflow identity")
	route := fs.String("route", "", "only this automatic route")
	groupBy := fs.String("group-by", "workflow", "aggregation: none, workflow, tier, provider, or day")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	start, end, err := parseReportWindow(*since, *from, *to)
	if err != nil {
		return fmtErr(stderr, err)
	}
	client, err := cf.client()
	if err != nil {
		return fmtErr(stderr, err)
	}
	ctx, cancel := contextWithTimeout()
	defer cancel()
	resp, err := client.Statistics(ctx, connect.NewRequest(&controlv1.StatisticsRequest{
		From: start, To: end, Tier: *tier, Provider: *provider, Repository: *repository,
		Workflow: *workflow, GroupBy: *groupBy, Route: *route,
	}))
	if err != nil {
		return fmtErr(stderr, err)
	}
	if cf.json {
		return printJSON(stdout, stderr, resp.Msg)
	}
	renderStatistics(stdout, resp.Msg)
	return 0
}

func renderStatistics(w io.Writer, stats *controlv1.StatisticsResponse) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	outln(tw, "GROUP\tJOBS\tOK\tFAILED\tINFRA\tIN_PROGRESS\tQUEUE_P50\tRUN_P50\tDIRECT_COST")
	for _, group := range stats.Groups {
		costs := make([]string, 0, len(group.DirectCosts))
		for _, cost := range group.DirectCosts {
			value := formatMoneyNanos(cost.Nanos, cost.Currency)
			if cost.UnknownEntries > 0 {
				value += " (+unknown)"
			}
			costs = append(costs, value)
		}
		outf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n",
			statisticsGroupLabel(group.Key), group.Jobs, group.Succeeded,
			group.Failed+group.Cancelled+group.Skipped, group.InfraFailed+group.Interrupted,
			group.InProgress, durationP50(group.QueueDuration), durationP50(group.RunDuration),
			emptyDash(strings.Join(costs, ", ")))
	}
	_ = tw.Flush()
	if len(stats.Groups) == 0 {
		outln(w, "(no job statistics)")
	}
	if len(stats.FleetCosts) == 0 {
		if len(stats.FleetTimings) == 0 && len(stats.RoutingEffectiveness) == 0 {
			return
		}
	} else {
		outln(w, "\nfleet costs:")
		tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		outln(tw, "DAY\tTIER\tPROVIDER\tKIND\tCOST\tUNKNOWN")
		for _, cost := range stats.FleetCosts {
			outf(tw, "%s\t%s\t%s\t%s\t%s\t%d\n", emptyDash(cost.Day), emptyDash(cost.Tier),
				emptyDash(cost.Provider), cost.Kind, formatMoneyNanos(cost.Nanos, cost.Currency), cost.UnknownEntries)
		}
		_ = tw.Flush()
	}
	if len(stats.FleetTimings) != 0 {
		outln(w, "\nfleet timings:")
		tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		outln(tw, "DAY\tTIER\tPROVIDER\tPHASE\tCOUNT\tP50\tP95\tTOTAL")
		for _, timing := range stats.FleetTimings {
			outf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n", emptyDash(timing.Day), emptyDash(timing.Tier),
				emptyDash(timing.Provider), timing.Kind, timing.Duration.GetCount(),
				formatProtoDuration(timing.Duration.GetP50()), formatProtoDuration(timing.Duration.GetP95()),
				formatProtoDuration(timing.Duration.GetTotal()))
		}
		_ = tw.Flush()
	}
	if len(stats.RoutingEffectiveness) == 0 {
		return
	}
	outln(w, "\nrouting effectiveness:")
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	outln(tw, "ROUTE\tJOBS\tCOMPLETED\tFALLBACK\tHISTORY\tIDLE\tP95_HIT\tESTIMATED\tSAVINGS\tACTUAL\tUNKNOWN\tSELECTIONS")
	for _, effectiveness := range stats.RoutingEffectiveness {
		outf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%s\n",
			effectiveness.Route, effectiveness.Decisions, effectiveness.Completed,
			effectiveness.FallbackDecisions, effectiveness.HistoryDecisions,
			effectiveness.IdleDecisions, routingHitRate(effectiveness.P95Hits, effectiveness.P95Misses),
			formatMoneyNanos(effectiveness.EstimatedSelectedNanos, effectiveness.Currency),
			formatMoneyNanos(effectiveness.EstimatedSavingsNanos, effectiveness.Currency),
			formatMoneyNanos(effectiveness.ActualDirectNanos, effectiveness.Currency),
			effectiveness.ActualUnknownEntries, routingSelections(effectiveness.Selections))
	}
	_ = tw.Flush()
}

func routingHitRate(hits, misses int64) string {
	total := hits + misses
	if total == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(hits)/float64(total))
}

func routingSelections(selections []*controlv1.RoutingSelection) string {
	values := make([]string, 0, len(selections))
	for _, selection := range selections {
		values = append(values, fmt.Sprintf("%s/%s:%d", selection.Tier, selection.Provider, selection.Jobs))
	}
	return emptyDash(strings.Join(values, ","))
}

func statisticsGroupLabel(key *controlv1.StatisticsKey) string {
	if key == nil {
		return "all"
	}
	for _, value := range []string{key.Workflow, key.Tier, key.Provider, key.Day, key.Repository} {
		if value != "" {
			return value
		}
	}
	return "all"
}

func durationP50(summary *controlv1.DurationSummary) string {
	if summary == nil || summary.Count == 0 || summary.P50 == nil {
		return "-"
	}
	return summary.P50.AsDuration().Round(time.Millisecond).String()
}

func formatProtoDuration(duration interface{ AsDuration() time.Duration }) string {
	if duration == nil {
		return "-"
	}
	return duration.AsDuration().Round(time.Millisecond).String()
}
