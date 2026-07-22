package digitalocean_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/digitalocean/godo"

	"github.com/hstern/fj-bellows/internal/provider/digitalocean"
	domock "github.com/hstern/fj-bellows/internal/provider/digitalocean/mock"
)

func TestQuoteUsesDigitalOceanSizeCatalog(t *testing.T) {
	fake := &domock.Client{FindSizeFn: func(_ context.Context, slug string) (godo.Size, error) {
		if slug != testSizeSlug {
			t.Fatalf("FindSize slug = %q", slug)
		}
		return godo.Size{Slug: slug, PriceHourly: 0.07143, PriceMonthly: 48}, nil
	}}
	d := configuredProvider(t, validConfig, fake)
	before := time.Now().UTC()
	quote, err := d.Quote(context.Background(), testSizeSlug)
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	after := time.Now().UTC()
	if quote.InstanceType != testSizeSlug || quote.Currency != "USD" {
		t.Fatalf("quote identity = %#v", quote)
	}
	if quote.PerHourNanos != 71_430_000 || quote.PerMonthNanos != 48_000_000_000 {
		t.Fatalf("quote prices = %#v", quote)
	}
	if quote.BillingQuantum != time.Second || quote.MinimumDuration != time.Minute {
		t.Fatalf("quote billing terms = %#v", quote)
	}
	if quote.MinimumChargeNanos != 10_000_000 {
		t.Fatalf("minimum charge = %d", quote.MinimumChargeNanos)
	}
	if quote.Source != "digitalocean:sizes" {
		t.Fatalf("source = %q", quote.Source)
	}
	if quote.ObservedAt.Before(before) || quote.ObservedAt.After(after) {
		t.Fatalf("observed at = %v, want between %v and %v", quote.ObservedAt, before, after)
	}
	if got := fake.FindSizeCalls(); len(got) != 1 || got[0] != testSizeSlug {
		t.Fatalf("size calls = %v", got)
	}
}

func TestQuoteCompleteOverrideSkipsCatalog(t *testing.T) {
	fake := &domock.Client{}
	d := configuredProvider(t, validConfig+`
pricing_override:
  currency: usd
  minimum_charge: "0.02"
  snapshot_gb_month: "0.06"
  instances:
    s-4vcpu-8gb:
      per_hour: "0.071234567"
      per_month: "47.5"
`, fake)
	quote, err := d.Quote(context.Background(), testSizeSlug)
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if quote.PerHourNanos != 71_234_567 || quote.PerMonthNanos != 47_500_000_000 {
		t.Fatalf("quote prices = %#v", quote)
	}
	if quote.SnapshotGBMonthNanos != 60_000_000 || quote.Source != "config:pricing_override" {
		t.Fatalf("quote override metadata = %#v", quote)
	}
	if quote.MinimumChargeNanos != 20_000_000 {
		t.Fatalf("minimum charge = %d", quote.MinimumChargeNanos)
	}
	if len(fake.FindSizeCalls()) != 0 {
		t.Fatalf("catalog calls = %v, want none", fake.FindSizeCalls())
	}
}

func TestQuotePartialOverrideMergesCatalog(t *testing.T) {
	fake := &domock.Client{FindSizeFn: func(context.Context, string) (godo.Size, error) {
		return godo.Size{PriceHourly: 1, PriceMonthly: 99}, nil
	}}
	d := configuredProvider(t, validConfig+`
pricing_override:
  instances:
    test-size:
      per_hour: "0.25"
`, fake)
	quote, err := d.Quote(context.Background(), "test-size")
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if quote.PerHourNanos != 250_000_000 || quote.PerMonthNanos != 99_000_000_000 {
		t.Fatalf("quote prices = %#v", quote)
	}
	if quote.Source != "digitalocean:sizes+config:pricing_override" {
		t.Fatalf("source = %q", quote.Source)
	}
	if len(fake.FindSizeCalls()) != 1 {
		t.Fatalf("catalog calls = %v", fake.FindSizeCalls())
	}
}

func TestConfigureRejectsInvalidPricingOverrides(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "currency", body: "currency: US", want: "three-letter"},
		{name: "foreign currency minimum", body: "currency: EUR\ninstances: {size: {per_hour: \"1\", per_month: \"2\"}}", want: "minimum_charge"},
		{name: "empty", body: "currency: USD", want: "must set instances"},
		{name: "empty rate", body: "instances: {size: {}}", want: "per_hour or per_month"},
		{name: "negative", body: "instances: {size: {per_hour: \"-1\"}}", want: "non-negative"},
		{name: "precision", body: "instances: {size: {per_hour: \"0.1234567890\"}}", want: "9 fractional"},
		{name: "exponent", body: "instances: {size: {per_hour: \"1e3\"}}", want: "decimal string"},
		{name: "empty slug", body: "instances: {\"\": {per_hour: \"1\"}}", want: "empty size slug"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig + "pricing_override:\n  " + strings.ReplaceAll(tt.body, "\n", "\n  ") + "\n"
			d := digitalocean.NewWithClient(&domock.Client{})
			err := d.Configure(context.Background(), testTag, configNode(t, cfg))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Configure error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestQuoteRejectsForeignCurrencyCatalogMix(t *testing.T) {
	d := configuredProvider(t, validConfig+`
pricing_override:
  currency: EUR
  minimum_charge: "0.01"
  instances:
    test-size:
      per_hour: "0.25"
`, &domock.Client{})
	_, err := d.Quote(context.Background(), "test-size")
	if err == nil || !strings.Contains(err.Error(), "provide both per_hour and per_month") {
		t.Fatalf("Quote error = %v", err)
	}
}

func TestQuotePropagatesCatalogAndPriceErrors(t *testing.T) {
	wantErr := errors.New("catalog unavailable")
	fake := &domock.Client{FindSizeFn: func(context.Context, string) (godo.Size, error) {
		return godo.Size{}, wantErr
	}}
	d := configuredProvider(t, validConfig, fake)
	if _, err := d.Quote(context.Background(), genericSize); !errors.Is(err, wantErr) {
		t.Fatalf("Quote error = %v", err)
	}

	fake.FindSizeFn = func(context.Context, string) (godo.Size, error) {
		return godo.Size{PriceHourly: math.NaN(), PriceMonthly: 1}, nil
	}
	if _, err := d.Quote(context.Background(), genericSize); err == nil || !strings.Contains(err.Error(), "hourly price") {
		t.Fatalf("Quote invalid-price error = %v", err)
	}
}

func TestQuoteRejectsEmptyInstanceType(t *testing.T) {
	d := configuredProvider(t, validConfig, &domock.Client{})
	if _, err := d.Quote(context.Background(), " "); err == nil {
		t.Fatal("Quote accepted an empty instance type")
	}
}
