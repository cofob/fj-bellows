package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func parseReportWindow(since, from, to string) (*timestamppb.Timestamp, *timestamppb.Timestamp, error) {
	if since != "" && from != "" {
		return nil, nil, errors.New("-since and -from are mutually exclusive")
	}
	var start, end time.Time
	var err error
	if since != "" {
		var duration time.Duration
		duration, err = time.ParseDuration(since)
		if err != nil || duration < 0 {
			return nil, nil, fmt.Errorf("parse -since %q: expected a non-negative duration", since)
		}
		start = time.Now().UTC().Add(-duration)
	}
	if from != "" {
		start, err = time.Parse(time.RFC3339, from)
		if err != nil {
			return nil, nil, fmt.Errorf("parse -from %q as RFC3339: %w", from, err)
		}
	}
	if to != "" {
		end, err = time.Parse(time.RFC3339, to)
		if err != nil {
			return nil, nil, fmt.Errorf("parse -to %q as RFC3339: %w", to, err)
		}
	}
	if !start.IsZero() && !end.IsZero() && !end.After(start) {
		return nil, nil, errors.New("-to must be after -from")
	}
	var startProto, endProto *timestamppb.Timestamp
	if !start.IsZero() {
		startProto = timestamppb.New(start)
	}
	if !end.IsZero() {
		endProto = timestamppb.New(end)
	}
	return startProto, endProto, nil
}

func formatMoneyNanos(nanos int64, currency string) string {
	whole := nanos / 1_000_000_000
	fraction := nanos % 1_000_000_000
	if fraction == 0 {
		return fmt.Sprintf("%d %s", whole, emptyDash(currency))
	}
	decimal := strings.TrimRight(fmt.Sprintf("%09d", fraction), "0")
	return fmt.Sprintf("%d.%s %s", whole, decimal, emptyDash(currency))
}

func protoInterval(start, end *timestamppb.Timestamp) string {
	if start == nil || end == nil || end.AsTime().Before(start.AsTime()) {
		return "-"
	}
	return end.AsTime().Sub(start.AsTime()).Round(time.Millisecond).String()
}
