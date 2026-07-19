// Package tenant provides the TenantID type and context propagation primitives
// used throughout xflow. TenantID is server-issued and must never be read from
// an untrusted client request body; handlers inject it into context and all
// downstream code reads it from context.
package tenant

import "context"

// TenantID identifies a tenant scope. It is issued by the server (e.g. from an
// authenticated principal) and must never be trusted when sent by a client.
type TenantID string

// DefaultTenant is the fallback tenant for single-tenant compatibility.
// Any context without an explicit tenant resolves to this value.
const DefaultTenant TenantID = "default"

type contextKey struct{}

var tenantKey = &contextKey{}

// WithTenant returns a new context that carries the given tenant.
func WithTenant(ctx context.Context, t TenantID) context.Context {
	return context.WithValue(ctx, tenantKey, t)
}

// FromContext returns the tenant carried by ctx, or DefaultTenant if no tenant
// is set or if the stored value is empty.
func FromContext(ctx context.Context) TenantID {
	if v, ok := ctx.Value(tenantKey).(TenantID); ok && v != "" {
		return v
	}
	return DefaultTenant
}
