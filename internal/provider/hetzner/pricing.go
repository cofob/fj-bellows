package hetzner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hstern/fj-bellows/internal/provider"
)

const (
	defaultCurrency = "EUR"
	catalogSource   = "hetzner:pricing"
	overrideSource  = "config:pricing_override"
	nanosPerUnit    = int64(1_000_000_000)
)

type pricingOverride struct {
	Currency        string                       `yaml:"currency"`
	Instances       map[string]priceRateOverride `yaml:"instances"`
	SnapshotGBMonth string                       `yaml:"snapshot_gb_month"`

	snapshotGBMonthNanos int64
	hasSnapshotRate      bool
}

type priceRateOverride struct {
	PerHour  string `yaml:"per_hour"`
	PerMonth string `yaml:"per_month"`

	perHourNanos  int64
	perMonthNanos int64
	hasPerHour    bool
	hasPerMonth   bool
}

//nolint:gocyclo // Validation reports each malformed pricing field with its exact YAML path.
func (p *pricingOverride) normalizeAndValidate() error {
	p.Currency = strings.ToUpper(strings.TrimSpace(p.Currency))
	if p.Currency == "" {
		p.Currency = defaultCurrency
	}
	if !validCurrency(p.Currency) {
		return fmt.Errorf("currency must be a three-letter ISO code, got %q", p.Currency)
	}
	normalized := make(map[string]priceRateOverride, len(p.Instances))
	for slug, rate := range p.Instances {
		slug = strings.TrimSpace(slug)
		if slug == "" {
			return errors.New("instances contains an empty server type")
		}
		if _, exists := normalized[slug]; exists {
			return fmt.Errorf("instances contains duplicate normalized server type %q", slug)
		}
		rate.PerHour = strings.TrimSpace(rate.PerHour)
		rate.PerMonth = strings.TrimSpace(rate.PerMonth)
		if rate.PerHour == "" && rate.PerMonth == "" {
			return fmt.Errorf("instances.%s must set per_hour or per_month", slug)
		}
		if rate.PerHour != "" {
			value, err := parseDecimalNanos(rate.PerHour)
			if err != nil {
				return fmt.Errorf("instances.%s.per_hour: %w", slug, err)
			}
			rate.perHourNanos, rate.hasPerHour = value, true
		}
		if rate.PerMonth != "" {
			value, err := parseDecimalNanos(rate.PerMonth)
			if err != nil {
				return fmt.Errorf("instances.%s.per_month: %w", slug, err)
			}
			rate.perMonthNanos, rate.hasPerMonth = value, true
		}
		normalized[slug] = rate
	}
	p.Instances = normalized
	p.SnapshotGBMonth = strings.TrimSpace(p.SnapshotGBMonth)
	if p.SnapshotGBMonth != "" {
		value, err := parseDecimalNanos(p.SnapshotGBMonth)
		if err != nil {
			return fmt.Errorf("snapshot_gb_month: %w", err)
		}
		p.snapshotGBMonthNanos, p.hasSnapshotRate = value, true
	}
	if len(p.Instances) == 0 && !p.hasSnapshotRate {
		return errors.New("must set instances or snapshot_gb_month")
	}
	return nil
}

// Quote returns a fixed-point list-price observation for one server type at
// the configured location. Overrides may replace either rate independently;
// incomplete overrides are merged only when their currency matches catalog.
//
//nolint:gocyclo // Quote explicitly merges independently optional fixed-point rates with catalog data.
func (h *Hetzner) Quote(ctx context.Context, instanceType string) (provider.PriceQuote, error) {
	if h.client == nil {
		return provider.PriceQuote{}, errors.New("hetzner: provider is not configured")
	}
	instanceType = strings.TrimSpace(instanceType)
	if instanceType == "" {
		return provider.PriceQuote{}, errors.New("hetzner: quote instance type must not be empty")
	}

	var override priceRateOverride
	var hasOverride bool
	var snapshotOverride int64
	var hasSnapshotOverride bool
	overrideCurrency := ""
	if h.cfg.PricingOverride != nil {
		overrideCurrency = h.cfg.PricingOverride.Currency
		override, hasOverride = h.cfg.PricingOverride.Instances[instanceType]
		snapshotOverride = h.cfg.PricingOverride.snapshotGBMonthNanos
		hasSnapshotOverride = h.cfg.PricingOverride.hasSnapshotRate
	}

	needsCatalog := !hasOverride || !override.hasPerHour || !override.hasPerMonth || !hasSnapshotOverride
	var currency string
	var perHour, perMonth, snapshot int64
	//nolint:nestif // Keeping catalog validation together prevents partially constructed mixed-currency quotes.
	if needsCatalog {
		catalog, err := h.client.GetPricing(ctx)
		if err != nil {
			return provider.PriceQuote{}, fmt.Errorf("hetzner: get pricing catalog: %w", err)
		}
		currency = strings.ToUpper(strings.TrimSpace(catalog.Currency))
		if !validCurrency(currency) {
			return provider.PriceQuote{}, fmt.Errorf("hetzner: pricing catalog returned invalid currency %q", catalog.Currency)
		}
		if overrideCurrency != "" && overrideCurrency != currency && (hasOverride || hasSnapshotOverride) {
			return provider.PriceQuote{}, fmt.Errorf("hetzner: cannot merge %s pricing override with %s catalog", overrideCurrency, currency)
		}
		var found bool
		for _, price := range catalog.ServerTypes {
			if price.InstanceType != instanceType || price.Location != h.cfg.Location {
				continue
			}
			perHour, err = parseDecimalNanos(price.PerHour)
			if err != nil {
				return provider.PriceQuote{}, fmt.Errorf("hetzner: catalog %s/%s hourly price: %w", instanceType, h.cfg.Location, err)
			}
			perMonth, err = parseDecimalNanos(price.PerMonth)
			if err != nil {
				return provider.PriceQuote{}, fmt.Errorf("hetzner: catalog %s/%s monthly price: %w", instanceType, h.cfg.Location, err)
			}
			found = true
			break
		}
		if !found {
			return provider.PriceQuote{}, fmt.Errorf("hetzner: server type %q has no catalog price in location %q", instanceType, h.cfg.Location)
		}
		snapshot, err = parseDecimalNanos(catalog.SnapshotGBMonth)
		if err != nil {
			return provider.PriceQuote{}, fmt.Errorf("hetzner: catalog snapshot price: %w", err)
		}
	} else {
		currency = overrideCurrency
	}

	source := catalogSource
	if hasOverride {
		if override.hasPerHour {
			perHour = override.perHourNanos
		}
		if override.hasPerMonth {
			perMonth = override.perMonthNanos
		}
	}
	if hasSnapshotOverride {
		snapshot = snapshotOverride
	}
	if hasOverride || hasSnapshotOverride {
		if needsCatalog {
			source += "+" + overrideSource
		} else {
			source = overrideSource
		}
	}

	now := time.Now
	if h.now != nil {
		now = h.now
	}
	return provider.PriceQuote{
		InstanceType:         instanceType,
		Currency:             currency,
		PerHourNanos:         perHour,
		PerMonthNanos:        perMonth,
		BillingQuantum:       time.Hour,
		MinimumDuration:      time.Hour,
		SnapshotGBMonthNanos: snapshot,
		Source:               source,
		ObservedAt:           now().UTC(),
	}, nil
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, char := range currency {
		if char < 'A' || char > 'Z' {
			return false
		}
	}
	return true
}

//nolint:gocyclo // Decimal parsing handles validation, overflow, padding, and rounding without float64.
func parseDecimalNanos(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	whole, fraction, hasFraction := strings.Cut(raw, ".")
	if raw == "" || whole == "" || strings.Contains(fraction, ".") || !allDigits(whole) ||
		(hasFraction && (fraction == "" || !allDigits(fraction))) {
		return 0, fmt.Errorf("must be a non-negative decimal string, got %q", raw)
	}
	wholeValue, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || wholeValue > math.MaxInt64/nanosPerUnit {
		return 0, fmt.Errorf("decimal value is too large: %q", raw)
	}
	kept := fraction
	if len(kept) > 9 {
		kept = kept[:9]
	}
	kept += strings.Repeat("0", 9-len(kept))
	var fractionalValue int64
	if kept != "" {
		fractionalValue, err = strconv.ParseInt(kept, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid decimal value %q: %w", raw, err)
		}
	}
	value := wholeValue*nanosPerUnit + fractionalValue
	if len(fraction) > 9 && fraction[9] >= '5' {
		if value == math.MaxInt64 {
			return 0, fmt.Errorf("decimal value is too large: %q", raw)
		}
		value++
	}
	return value, nil
}

func allDigits(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
