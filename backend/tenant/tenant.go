// Package tenant provides the TenantID type and context propagation primitives
// used throughout xflow. TenantID is server-issued and must never be read from
// an untrusted client request body; handlers inject it into context and all
// downstream code reads it from context.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// TenantID identifies a tenant scope. It is issued by the server (e.g. from an
// authenticated principal) and must never be trusted when sent by a client.
type TenantID string

// DefaultTenant is the fallback tenant for single-tenant compatibility.
// Any context without an explicit tenant resolves to this value.
const DefaultTenant TenantID = "default"

// MaxNameLen is the maximum length of a tenant name. It keeps Redis keys
// bounded and avoids abuse of the tenant namespace prefix.
const MaxNameLen = 64

// Validation errors returned by Validate.
var (
	ErrTenantNameEmpty   = errors.New("tenant name must not be empty")
	ErrTenantNameTooLong = fmt.Errorf("tenant name must not exceed %d characters", MaxNameLen)
	ErrTenantNameChars   = errors.New("tenant name must not contain ':', '{', or '}'")
)

type contextKey struct{}

var tenantKey = &contextKey{}

// Validate checks that t is safe to embed in Redis keys. It rejects empty
// names, names that exceed MaxNameLen, and names containing the key-schema
// delimiter characters ':', '{', or '}'. The empty tenant is normalized to
// DefaultTenant by FromContext and registerTenant; callers that need to
// validate an explicit tenant should normalize first or expect ErrTenantNameEmpty.
func Validate(t TenantID) error {
	s := string(t)
	if s == "" {
		return ErrTenantNameEmpty
	}
	if len(s) > MaxNameLen {
		return ErrTenantNameTooLong
	}
	if strings.ContainsAny(s, ":{}") {
		return ErrTenantNameChars
	}
	return nil
}

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
