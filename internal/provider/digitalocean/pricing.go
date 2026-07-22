package digitalocean

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
	defaultCurrency    = "USD"
	catalogSource      = "digitalocean:sizes"
	overrideSource     = "config:pricing_override"
	nanosPerUnit       = int64(1_000_000_000)
	minimumChargeNanos = int64(10_000_000)
)

type pricingOverride struct {
	Currency        string                       `yaml:"currency"`
	Instances       map[string]priceRateOverride `yaml:"instances"`
	MinimumCharge   string                       `yaml:"minimum_charge"`
	SnapshotGBMonth string                       `yaml:"snapshot_gb_month"`

	minimumChargeNanos   int64
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

func (p *pricingOverride) normalizeAndValidate() error {
	if err := p.normalizeCurrencyAndMinimum(); err != nil {
		return err
	}
	normalized, err := normalizeInstanceRates(p.Instances)
	if err != nil {
		return err
	}
	p.Instances = normalized
	if err := p.normalizeSnapshotRate(); err != nil {
		return err
	}
	if len(p.Instances) == 0 && !p.hasSnapshotRate {
		return errors.New("must set instances or snapshot_gb_month")
	}
	return nil
}

func (p *pricingOverride) normalizeCurrencyAndMinimum() error {
	p.Currency = strings.ToUpper(strings.TrimSpace(p.Currency))
	if p.Currency == "" {
		p.Currency = defaultCurrency
	}
	if !validCurrency(p.Currency) {
		return fmt.Errorf("currency must be a three-letter ISO code, got %q", p.Currency)
	}
	p.MinimumCharge = strings.TrimSpace(p.MinimumCharge)
	if p.MinimumCharge == "" {
		if p.Currency != defaultCurrency {
			return errors.New("minimum_charge is required when currency is not USD")
		}
		p.minimumChargeNanos = minimumChargeNanos
	} else {
		value, err := parseDecimalNanos(p.MinimumCharge)
		if err != nil {
			return fmt.Errorf("minimum_charge: %w", err)
		}
		p.minimumChargeNanos = value
	}
	return nil
}

func normalizeInstanceRates(instances map[string]priceRateOverride) (map[string]priceRateOverride, error) {
	normalized := make(map[string]priceRateOverride, len(instances))
	for slug, rate := range instances {
		slug = strings.TrimSpace(slug)
		if slug == "" {
			return nil, errors.New("instances contains an empty size slug")
		}
		if _, exists := normalized[slug]; exists {
			return nil, fmt.Errorf("instances contains duplicate normalized size slug %q", slug)
		}
		normalizedRate, err := normalizeInstanceRate(slug, rate)
		if err != nil {
			return nil, err
		}
		normalized[slug] = normalizedRate
	}
	return normalized, nil
}

func normalizeInstanceRate(slug string, rate priceRateOverride) (priceRateOverride, error) {
	rate.PerHour = strings.TrimSpace(rate.PerHour)
	rate.PerMonth = strings.TrimSpace(rate.PerMonth)
	if rate.PerHour == "" && rate.PerMonth == "" {
		return priceRateOverride{}, fmt.Errorf("instances.%s must set per_hour or per_month", slug)
	}
	if rate.PerHour != "" {
		value, err := parseDecimalNanos(rate.PerHour)
		if err != nil {
			return priceRateOverride{}, fmt.Errorf("instances.%s.per_hour: %w", slug, err)
		}
		rate.perHourNanos = value
		rate.hasPerHour = true
	}
	if rate.PerMonth != "" {
		value, err := parseDecimalNanos(rate.PerMonth)
		if err != nil {
			return priceRateOverride{}, fmt.Errorf("instances.%s.per_month: %w", slug, err)
		}
		rate.perMonthNanos = value
		rate.hasPerMonth = true
	}
	return rate, nil
}

func (p *pricingOverride) normalizeSnapshotRate() error {
	p.SnapshotGBMonth = strings.TrimSpace(p.SnapshotGBMonth)
	if p.SnapshotGBMonth != "" {
		value, err := parseDecimalNanos(p.SnapshotGBMonth)
		if err != nil {
			return fmt.Errorf("snapshot_gb_month: %w", err)
		}
		p.snapshotGBMonthNanos = value
		p.hasSnapshotRate = true
	}
	return nil
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

func parseDecimalNanos(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	whole, fraction, hasFraction := strings.Cut(raw, ".")
	if raw == "" || whole == "" || strings.Contains(fraction, ".") {
		return 0, fmt.Errorf("must be a non-negative decimal string, got %q", raw)
	}
	if !allDigits(whole) || (hasFraction && (fraction == "" || !allDigits(fraction))) {
		return 0, fmt.Errorf("must be a non-negative decimal string, got %q", raw)
	}
	if len(fraction) > 9 {
		return 0, fmt.Errorf("has more than 9 fractional digits: %q", raw)
	}

	wholeValue, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || wholeValue > math.MaxInt64/nanosPerUnit {
		return 0, fmt.Errorf("decimal value is too large: %q", raw)
	}
	fraction += strings.Repeat("0", 9-len(fraction))
	var fractionValue int64
	if fraction != "" {
		fractionValue, err = strconv.ParseInt(fraction, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid decimal value %q: %w", raw, err)
		}
	}
	value := wholeValue*nanosPerUnit + fractionValue
	if value < 0 {
		return 0, fmt.Errorf("decimal value is too large: %q", raw)
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

type quoteOverride struct {
	currency           string
	rate               priceRateOverride
	hasRate            bool
	minimumChargeNanos int64
	snapshotRateNanos  int64
	hasSnapshotRate    bool
}

func (d *DigitalOcean) resolveQuoteOverride(instanceType string) quoteOverride {
	resolved := quoteOverride{
		currency:           defaultCurrency,
		minimumChargeNanos: minimumChargeNanos,
	}
	if d.cfg.PricingOverride == nil {
		return resolved
	}
	pricing := d.cfg.PricingOverride
	resolved.currency = pricing.Currency
	resolved.rate, resolved.hasRate = pricing.Instances[instanceType]
	resolved.minimumChargeNanos = pricing.minimumChargeNanos
	resolved.snapshotRateNanos = pricing.snapshotGBMonthNanos
	resolved.hasSnapshotRate = pricing.hasSnapshotRate
	return resolved
}

func (o quoteOverride) needsCatalog() bool {
	return !o.hasRate || !o.rate.hasPerHour || !o.rate.hasPerMonth
}

func (o quoteOverride) applyRates(perHour, perMonth int64) (int64, int64) {
	if !o.hasRate {
		return perHour, perMonth
	}
	if o.rate.hasPerHour {
		perHour = o.rate.perHourNanos
	}
	if o.rate.hasPerMonth {
		perMonth = o.rate.perMonthNanos
	}
	return perHour, perMonth
}

func (o quoteOverride) source(needsCatalog bool) string {
	switch {
	case o.hasRate && !needsCatalog:
		return overrideSource
	case o.hasRate || o.hasSnapshotRate:
		return catalogSource + "+" + overrideSource
	default:
		return catalogSource
	}
}

func (d *DigitalOcean) catalogRates(ctx context.Context, instanceType string) (int64, int64, error) {
	size, err := d.client.FindSize(ctx, instanceType)
	if err != nil {
		return 0, 0, fmt.Errorf("digitalocean: quote size %q: %w", instanceType, err)
	}
	perHour, err := floatPriceToNanos(size.PriceHourly)
	if err != nil {
		return 0, 0, fmt.Errorf("digitalocean: size %q hourly price: %w", instanceType, err)
	}
	perMonth, err := floatPriceToNanos(size.PriceMonthly)
	if err != nil {
		return 0, 0, fmt.Errorf("digitalocean: size %q monthly price: %w", instanceType, err)
	}
	return perHour, perMonth, nil
}

// Quote returns an immutable DigitalOcean list-price observation for a size.
// Complete configured overrides avoid a catalog call; partial overrides are
// merged over the catalog result.
func (d *DigitalOcean) Quote(ctx context.Context, instanceType string) (provider.PriceQuote, error) {
	if d.client == nil {
		return provider.PriceQuote{}, errors.New("digitalocean: provider is not configured")
	}
	instanceType = strings.TrimSpace(instanceType)
	if instanceType == "" {
		return provider.PriceQuote{}, errors.New("digitalocean: quote instance type must not be empty")
	}

	override := d.resolveQuoteOverride(instanceType)
	needsCatalog := override.needsCatalog()
	if needsCatalog && override.currency != defaultCurrency {
		return provider.PriceQuote{}, fmt.Errorf(
			"digitalocean: quote size %q: non-USD pricing overrides must provide both per_hour and per_month",
			instanceType,
		)
	}
	var perHour, perMonth int64
	if needsCatalog {
		var err error
		perHour, perMonth, err = d.catalogRates(ctx, instanceType)
		if err != nil {
			return provider.PriceQuote{}, err
		}
	}
	perHour, perMonth = override.applyRates(perHour, perMonth)

	now := time.Now
	if d.now != nil {
		now = d.now
	}
	return provider.PriceQuote{
		InstanceType:         instanceType,
		Currency:             override.currency,
		PerHourNanos:         perHour,
		PerMonthNanos:        perMonth,
		BillingQuantum:       time.Second,
		MinimumDuration:      time.Minute,
		MinimumChargeNanos:   override.minimumChargeNanos,
		SnapshotGBMonthNanos: override.snapshotRateNanos,
		Source:               override.source(needsCatalog),
		ObservedAt:           now().UTC(),
	}, nil
}

func floatPriceToNanos(price float64) (int64, error) {
	if math.IsNaN(price) || math.IsInf(price, 0) || price < 0 {
		return 0, fmt.Errorf("invalid non-negative price %v", price)
	}
	if price > float64(math.MaxInt64)/float64(nanosPerUnit) {
		return 0, fmt.Errorf("price is too large: %v", price)
	}
	return int64(math.Round(price * float64(nanosPerUnit))), nil
}
