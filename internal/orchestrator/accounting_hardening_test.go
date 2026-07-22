package orchestrator

import (
	"context"
	"testing"
	"time"

	omock "github.com/hstern/fj-bellows/internal/orchestrator/mock"
	pmock "github.com/hstern/fj-bellows/internal/provider/mock"
	"github.com/hstern/fj-bellows/internal/storage"
	smock "github.com/hstern/fj-bellows/internal/storage/mock"
)

func TestRecordResourceCostAppliesCapPerBillingMonth(t *testing.T) {
	const quoteID = int64(501)
	// The allocation touches twelve hours of January, all of February, and
	// twelve hours of March. At 10 nanos/hour with a 1,000-nano monthly cap,
	// those calendar-month charges are 120 + 1,000 + 120.
	start := time.Date(2026, time.January, 31, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	var recorded []storage.CostEntry
	store := &smock.Store{
		GetPriceQuoteFn: func(_ context.Context, id int64) (storage.PriceQuote, error) {
			if id != quoteID {
				t.Errorf("GetPriceQuote ID = %d, want %d", id, quoteID)
			}
			return storage.PriceQuote{
				ID: quoteID, Currency: testCostCurrency, PerHourNanos: 10, PerMonthNanos: 1000,
			}, nil
		},
		RecordCostFn: func(_ context.Context, entry storage.CostEntry) (storage.CostEntry, error) {
			recorded = append(recorded, entry)
			return entry, nil
		},
	}
	o := New(baseConfig(), &pmock.Provider{}, &omock.JobSource{}, &omock.Dispatcher{}, nil)
	o.SetStore(store)

	o.recordResourceCost(t.Context(), Node{
		InstanceID: "multi-month", ResourceID: 500, PriceQuoteID: quoteID, CreatedAt: start,
	}, storage.CostBilledCompute, end)

	if len(recorded) != 1 {
		t.Fatalf("recorded costs = %+v, want one billed-compute entry", recorded)
	}
	entry := recorded[0]
	if entry.Kind != storage.CostBilledCompute || !entry.Known || entry.Currency != testCostCurrency {
		t.Fatalf("recorded billed cost = %+v", entry)
	}
	if entry.Nanos != 1240 {
		t.Fatalf("multi-month billed cost = %d, want 1240", entry.Nanos)
	}
}

func TestResourceBilledNanosAccruesEachFullMonthCap(t *testing.T) {
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	quote := storage.PriceQuote{PerHourNanos: 10, PerMonthNanos: 1000}
	if got := resourceBilledNanos(quote, start, end); got != 2000 {
		t.Fatalf("resourceBilledNanos() = %d, want two monthly caps (2000)", got)
	}
}
