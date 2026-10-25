package xbcinfra

import (
	"context"
	"log/slog"
	"strings"

	xbclog "github.com/xbcio/xbc/log"
)

// The slog bridge is host-side on purpose. xbc has no slog package: its log
// facade is the only logging contract, and a host whose libraries log through
// slog owns the adapter. The runner SDK logs through slog throughout, so this
// is what lets --xbc-config route those records through the facade backend,
// inheriting its level filtering, sensitive-field masking, and sink rotation
// instead of escaping to a second, separately configured logger.

// newSlogBridge returns a slog.Handler that writes through logger. Records are
// translated into the facade's flat KV form: attributes attached with WithAttrs
// or scoped by WithGroup are flattened onto every record, and a group becomes a
// dotted key prefix ("request.id") because the facade takes a flat KV sequence
// rather than nested objects. LogValuer values are resolved before they reach
// the backend. A nil logger is treated as log.Nop.
//
// The context passed to Handle is not ignored: a logger bound to it with
// log.NewContext wins, and a context carrying only a trace enriches the record
// with trace_id, span_id, and request_id, so a record logged from a request
// path stays correlated with the request that produced it. Resolution is
// log.CtxWith(ctx, logger), so the handler's own logger -- not the
// process-global one -- is the fallback. The record time and source PC are
// dropped: the backend stamps the entry when it writes, and the caller frame
// belongs to the backend's own configuration. Handle never fails; a record the
// backend declines by level is simply not written.
func newSlogBridge(logger xbclog.Logger) slog.Handler {
	if logger == nil {
		logger = xbclog.Nop()
	}
	return &slogBridge{logger: logger}
}

type slogBridge struct {
	logger xbclog.Logger
	bound  []boundAttrs
	groups []string
}

// boundAttrs holds attributes attached by one WithAttrs call together with the
// group prefix that was active when they were attached. Keeping the prefix with
// the attributes preserves slog's ordering rule: a WithGroup after WithAttrs
// scopes later records, not attributes that were already bound.
type boundAttrs struct {
	groups []string
	attrs  []slog.Attr
}

func (h *slogBridge) Enabled(_ context.Context, level slog.Level) bool {
	return h.logger.Enabled(facadeLevel(level))
}

func (h *slogBridge) Handle(ctx context.Context, record slog.Record) error {
	level := facadeLevel(record.Level)
	// Level filtering follows the handler's own logger: it is allocation-free,
	// and the context logger normally shares the same backend. The context then
	// decides where the record goes and which request fields it carries.
	if !h.logger.Enabled(level) {
		return nil
	}
	logger := xbclog.CtxWith(ctx, h.logger)

	kv := make([]any, 0, 2*(len(h.bound)+record.NumAttrs()))
	for _, bound := range h.bound {
		kv = appendAttrs(kv, bound.groups, bound.attrs)
	}
	record.Attrs(func(attr slog.Attr) bool {
		kv = appendAttr(kv, h.groups, attr)
		return true
	})

	switch level {
	case xbclog.DebugLevel:
		logger.Debug(record.Message, kv...)
	case xbclog.WarnLevel:
		logger.Warn(record.Message, kv...)
	case xbclog.ErrorLevel:
		logger.Error(record.Message, kv...)
	default:
		logger.Info(record.Message, kv...)
	}
	return nil
}

func (h *slogBridge) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	child := *h
	child.bound = append(append([]boundAttrs(nil), h.bound...), boundAttrs{
		groups: append([]string(nil), h.groups...),
		attrs:  append([]slog.Attr(nil), attrs...),
	})
	return &child
}

func (h *slogBridge) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	child := *h
	child.groups = append(append([]string(nil), h.groups...), name)
	return &child
}

// facadeLevel maps a slog level onto the nearest facade level. slog levels are
// integers whose named values are Debug=-4, Info=0, Warn=4, Error=8; custom
// levels fall into the band they sit in, and levels above Error stay Error
// because the facade has no higher severity that does not terminate the
// process.
func facadeLevel(level slog.Level) xbclog.Level {
	switch {
	case level < slog.LevelInfo:
		return xbclog.DebugLevel
	case level < slog.LevelWarn:
		return xbclog.InfoLevel
	case level < slog.LevelError:
		return xbclog.WarnLevel
	default:
		return xbclog.ErrorLevel
	}
}

// appendAttrs flattens attrs into kv under groups. Group values recurse, and an
// empty group name contributes no prefix, matching WithGroup's treatment of an
// empty name.
func appendAttrs(kv []any, groups []string, attrs []slog.Attr) []any {
	for _, attr := range attrs {
		kv = appendAttr(kv, groups, attr)
	}
	return kv
}

func appendAttr(kv []any, groups []string, attr slog.Attr) []any {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return kv
	}
	if attr.Value.Kind() == slog.KindGroup {
		if attr.Key != "" {
			groups = append(groups, attr.Key)
		}
		return appendAttrs(kv, groups, attr.Value.Group())
	}

	key := attr.Key
	if len(groups) > 0 {
		key = strings.Join(groups, ".") + "." + key
	}
	return append(kv, key, attr.Value.Any())
}
