package tracing

import (
	"context"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// BaggagePolicy bounds what W3C baggage the propagator accepts on Extract.
// Baggage is opt-in (see ProviderConfig.Baggage) precisely because it can
// carry unbounded, cross-tenant, or secret data; this policy enforces a key
// denylist, per-value length cap, entry-count cap, and total serialized size
// cap so a malicious or misbehaving peer cannot smuggle sensitive material or
// exhaust server memory through baggage.
type BaggagePolicy struct {
	// ForbiddenKeys are always rejected, regardless of AllowedKeys. They cover
	// the security-policy §3 / RELEASE-GATES §4.1 tenants: identity, secrets,
	// and request payloads must never ride in baggage. Tenant travels as a span
	// attribute only.
	ForbiddenKeys []string
	// AllowedKeys, when non-empty, is an allowlist: only these keys are
	// accepted. When empty, every key not in ForbiddenKeys is accepted (default
	// open + denylist). Operators who want strict allowlist behavior set this.
	AllowedKeys []string
	// MaxValueLen truncates each value to at most this many bytes. <=0 means
	// unlimited (not recommended).
	MaxValueLen int
	// MaxEntries caps the number of baggage entries accepted; surplus entries
	// are dropped. <=0 means unlimited.
	MaxEntries int
	// MaxTotalBytes caps the sum of accepted key+value lengths. Surplus entries
	// are dropped once the cap is exceeded. <=0 means unlimited.
	MaxTotalBytes int
}

// DefaultBaggagePolicy returns a safe default policy: tenant/token/payload
// and common secret-ish keys are denied, values are capped at 1KiB, at most 16
// entries, and 8KiB total.
func DefaultBaggagePolicy() BaggagePolicy {
	return BaggagePolicy{
		ForbiddenKeys: []string{
			"tenant", "tenant_id", "xflow.tenant",
			"token", "auth_token", "access_token", "refresh_token",
			"payload", "body", "secret", "credential", "ak", "sk",
		},
		MaxValueLen:    1024,
		MaxEntries:     16,
		MaxTotalBytes:  8 * 1024,
	}
}

func (p BaggagePolicy) isForbidden(key string) bool {
	lk := strings.ToLower(strings.TrimSpace(key))
	for _, f := range p.ForbiddenKeys {
		if lk == f {
			return true
		}
	}
	return false
}

func (p BaggagePolicy) isAllowed(key string) bool {
	if len(p.AllowedKeys) == 0 {
		return true
	}
	for _, a := range p.AllowedKeys {
		if key == a {
			return true
		}
	}
	return false
}

// carrierParseFailures counts malformed W3C carriers observed on Extract. It
// is the "fallback metric" the B1 contract requires: a malformed traceparent
// must not panic and must produce an observable, non-zero signal so operators
// can detect broken propagation.
var carrierParseFailures atomic.Int64

// CarrierParseFailures returns the cumulative count of malformed carriers
// observed since process start. Exposed for tests and diagnostics.
func CarrierParseFailures() int64 { return carrierParseFailures.Load() }

// recordCarrierParseFailure increments the malformed-carrier counter.
func recordCarrierParseFailure() { carrierParseFailures.Add(1) }

// filteredBaggage is a propagation.TextMapPropagator that wraps the standard
// W3C Baggage propagator and applies BaggagePolicy on Extract. Inject is
// passthrough: an in-process caller that sets baggage is trusted (the policy
// gates inbound, peer-supplied baggage).
type filteredBaggage struct {
	inner  propagation.TextMapPropagator
	policy BaggagePolicy
}

// FilteredBaggagePropagator returns a Baggage propagator that enforces policy
// on extraction. Use as the baggage component of a composite propagator.
func FilteredBaggagePropagator(policy BaggagePolicy) propagation.TextMapPropagator {
	return filteredBaggage{inner: propagation.Baggage{}, policy: policy}
}

func (f filteredBaggage) Inject(ctx context.Context, c propagation.TextMapCarrier) {
	f.inner.Inject(ctx, c)
}

func (f filteredBaggage) Extract(ctx context.Context, g propagation.TextMapCarrier) context.Context {
	// First extract with the standard propagator to parse the baggage header
	// (it tolerates malformed input without panicking), then rebuild a filtered
	// baggage list.
	extracted := f.inner.Extract(ctx, g)
	bag := baggage.FromContext(extracted)
	members := bag.Members()
	if len(members) == 0 {
		return ctx
	}

	allowed := make([]baggage.Member, 0, len(members))
	total := 0
	for _, m := range members {
		key := m.Key()
		if f.policy.isForbidden(key) {
			recordCarrierParseFailure()
			continue
		}
		if !f.policy.isAllowed(key) {
			recordCarrierParseFailure()
			continue
		}
		value := m.Value()
		if f.policy.MaxValueLen > 0 && len(value) > f.policy.MaxValueLen {
			// Truncate by reconstructing the member; a member with the same
			// key and a shortened value. If reconstruction fails (e.g. invalid
			// key after truncation), drop the entry rather than panicking.
			truncated := value[:f.policy.MaxValueLen]
			nm, err := baggage.NewMember(key, truncated)
			if err != nil {
				recordCarrierParseFailure()
				continue
			}
			m = nm
			value = truncated
		}
		if f.policy.MaxEntries > 0 && len(allowed) >= f.policy.MaxEntries {
			recordCarrierParseFailure()
			break
		}
		entrySize := len(key) + len(value)
		if f.policy.MaxTotalBytes > 0 && total+entrySize > f.policy.MaxTotalBytes {
			recordCarrierParseFailure()
			break
		}
		total += entrySize
		allowed = append(allowed, m)
	}
	if len(allowed) == 0 {
		return ctx
	}
	filtered, err := baggage.New(allowed...)
	if err != nil {
		recordCarrierParseFailure()
		return ctx
	}
	return baggage.ContextWithBaggage(ctx, filtered)
}

func (f filteredBaggage) Fields() []string { return f.inner.Fields() }
