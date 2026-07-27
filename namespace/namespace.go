// Package namespace provides the security namespace type and context
// propagation primitives used throughout xflow. Namespace is server-issued and
// must never be trusted when supplied by an untrusted client request body.
package namespace

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Namespace identifies an isolation scope. It is issued by the server, for
// example from an authenticated principal.
type Namespace string

// Default is the fallback namespace for single-namespace compatibility.
// Any context without an explicit namespace resolves to this value.
const Default Namespace = "default"

// MaxNameLen is the maximum length of a namespace name. It keeps Redis keys
// bounded and avoids abuse of the namespace prefix.
const MaxNameLen = 64

// Validation errors returned by Validate.
var (
	ErrNameEmpty   = errors.New("namespace name must not be empty")
	ErrNameTooLong = fmt.Errorf("namespace name must not exceed %d characters", MaxNameLen)
	ErrNameChars   = errors.New("namespace name must not contain ':', '{', '}', '*', '?', '[', ']', or '\\'")
)

type contextKey struct{}

var namespaceKey = &contextKey{}

// Validate checks that ns is safe to embed in Redis keys. It rejects empty
// names, names that exceed MaxNameLen, and names containing the key-schema
// delimiter characters ':', '{', '}' or Redis glob meta-characters '*', '?',
// '[', ']', '\\'.
func Validate(ns Namespace) error {
	s := string(ns)
	if s == "" {
		return ErrNameEmpty
	}
	if len(s) > MaxNameLen {
		return ErrNameTooLong
	}
	if strings.ContainsAny(s, ":{}*?[]\\") {
		return ErrNameChars
	}
	return nil
}

// WithNamespace returns a new context that carries the given namespace.
func WithNamespace(ctx context.Context, ns Namespace) context.Context {
	return context.WithValue(ctx, namespaceKey, ns)
}

// FromContext returns the namespace carried by ctx, or Default if no namespace
// is set or if the stored value is empty.
func FromContext(ctx context.Context) Namespace {
	if v, ok := ctx.Value(namespaceKey).(Namespace); ok && v != "" {
		return v
	}
	return Default
}
