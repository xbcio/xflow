package wasm

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// The guest's own account of a call-level failure, surfaced to the host log.
//
// Every negative return code the guest can produce is preceded by the guest
// writing a structured reason into its out buffer, and readOut has always
// copied it into reactorEvalError.detail. Nothing read that field: the struct's
// comment said "for host-side logging only" and the logging did not exist. So
// the reason a record was rejected was computed inside the sandbox, carried
// across the ABI boundary, and dropped — leaving only Error()'s per-code text,
// which is the same five sentences no matter what actually went wrong.
//
// Measured cost of that gap: against live SAS traffic, 93.6% of batch
// admissions failed with "wasm reactor: input decode failed" and nothing could
// say which of the guest's four errDecode branches produced it.
//
// This logs the detail; it deliberately does NOT put it in Error(). Error()
// flows into a map batch's _error entry, which reaches the node's output and
// can be persisted — and a guest is free to quote its input when explaining a
// rejection. A log line is the right home for a value with a guest's trust
// level and a stack trace's shape; a database column is not.
const (
	// evalDetailLogInterval bounds emissions per distinct reason. A malformed
	// producer invalidates every record in a partition, so unthrottled this
	// would be the loudest thing in the process.
	evalDetailLogInterval = 30 * time.Second

	// evalDetailLimit bounds one logged reason. The guest writes it, so treat
	// it with a stack trace's trust and keep it short.
	evalDetailLimit = 256

	// evalDetailKeyCap bounds how many distinct reasons get their own throttle
	// slot, so a guest emitting a unique message per record (an offset, an ID)
	// cannot grow the map without bound. Beyond the cap everything shares one
	// slot and the emitted line says so — see logEvalDetail.
	evalDetailKeyCap = 64
)

// evalDetailLogger throttles per DISTINCT REASON, not per error code.
//
// Keying on the code alone would defeat the purpose. The guest returns
// errDecode from four different branches — a bad envelope, an unparseable
// value, a missing $item, an unrecognised shape — and a code-keyed throttle
// would emit whichever branch fired first and suppress the rest for the whole
// interval. The operator would read one branch's text and take it for the
// whole population, which is precisely the mistake the flat admission counter
// already caused once.
type evalDetailLogger struct {
	mu         sync.Mutex
	last       map[string]time.Time
	suppressed map[string]int
	// capped records that at least one reason had to share the overflow slot,
	// so a throttled line can admit its key set was truncated rather than
	// quietly under-reporting the variety.
	capped bool
}

var evalDetailLog = &evalDetailLogger{
	last:       make(map[string]time.Time),
	suppressed: make(map[string]int),
}

// allow reports whether to emit now, how many occurrences the line stands for,
// and whether the key set hit its cap.
func (l *evalDetailLogger) allow(now time.Time, key string) (emit bool, occurrences int, capped bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, known := l.last[key]; !known && len(l.last) >= evalDetailKeyCap {
		key = "\x00overflow"
		l.capped = true
	}
	last, seen := l.last[key]
	if seen && now.Sub(last) < evalDetailLogInterval {
		l.suppressed[key]++
		return false, 0, l.capped
	}
	count := 1 + l.suppressed[key]
	l.suppressed[key] = 0
	l.last[key] = now
	return true, count, l.capped
}

// logEvalDetail emits the guest's reason for one call-level failure.
//
// op names the guest export that failed ("eval" or "configure"): the same code
// means different things across them — errDecode from configure is a malformed
// rule set, from eval it is a malformed record — and one is an operator's
// problem while the other is a producer's.
func logEvalDetail(op string, code int32, detail []byte) {
	reason := decodeEvalDetail(detail)
	if reason == "" {
		// A guest without out_len, or one that returned a code without writing
		// a reason. Nothing to add beyond what Error() already says.
		return
	}
	emit, occurrences, capped := evalDetailLog.allow(time.Now(), op+"\x00"+reason)
	if !emit {
		return
	}
	slog.Warn("wasm reactor rejected a call",
		"op", op,
		"code", code,
		"reason", reason,
		"occurrences", occurrences,
		"reasons_capped", capped,
	)
}

// decodeEvalDetail extracts the message from the guest's structured detail.
//
// ONLY the documented shape is accepted. §4.2 says a guest writes a structured
// error detail JSON into its out buffer alongside a negative code, and anything
// else is refused — including plain text that would read perfectly well in a log.
//
// That looks over-strict until you see what the alternative admits. out_len is
// assigned only when a guest writes output, so a guest that returns a negative
// code WITHOUT writing a reason leaves out_len holding the length of its last
// SUCCESSFUL call. readOut then returns that call's output, and a host willing
// to log whatever it gets would print one message's payload as another message's
// failure reason.
//
// SAS's decode guest did exactly this on ERR_OUTPUT, and the first run with
// logging enabled produced 23 lines whose "reason" was a serialised request and
// response body. Wrong content attributed to the wrong call, and live traffic in
// a log line.
//
// The host cannot make a guest compliant. It can decline to treat unattributable
// bytes as an explanation. An {"error": ...} envelope is something only this
// call could have written; a bare byte string is not, and the cost of being
// wrong is not symmetric — a missing log line versus a misattributed one.
func decodeEvalDetail(detail []byte) string {
	if len(detail) == 0 {
		return ""
	}
	var structured struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(detail, &structured); err != nil || structured.Error == "" {
		return ""
	}
	return truncateDetail(structured.Error)
}

func truncateDetail(s string) string {
	if len(s) <= evalDetailLimit {
		return s
	}
	return s[:evalDetailLimit] + "…(truncated)"
}
