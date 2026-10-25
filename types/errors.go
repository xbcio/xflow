package types

import "errors"

// ErrPermanent is the sentinel marker for an error that must NOT be retried.
// Use errors.Is(err, ErrPermanent) to check, errors.Join(ErrPermanent, err) to
// stamp an arbitrary error as permanent. Unmarked errors are treated as
// transient by retry-capable engines and queues.
var ErrPermanent = errors.New("xflow: permanent error")

// IsPermanent reports whether err should bypass retries.
func IsPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}

// ErrExecutionStatusUnavailable is the answer an ExecutionStatusReader gives
// when it has no execution store to read: the capability is present, but this
// runtime cannot answer the question. It is deliberately not a "not found" —
// found=false already means the execution is gone, and a caller must be able to
// tell "the run finished" from "I cannot see the run" — and not a transient
// failure, so it is never retried. Callers degrade to the behaviour that needs
// no status, exactly as they do when the capability is absent altogether.
var ErrExecutionStatusUnavailable = errors.New("xflow: execution status unavailable")

// ErrorKind classifies the source of a failure for retry and routing policy.
// The four kinds below are the whole taxonomy; each carries its own rule.
type ErrorKind string

const (
	// ErrorKindTransient marks a transport or system failure that may succeed
	// on retry (connection refused, timeout, 5xx).
	ErrorKindTransient ErrorKind = "transient"
	// ErrorKindPermanent marks a configuration or input failure that will not
	// succeed on retry (bad params, 4xx client error).
	ErrorKindPermanent ErrorKind = "permanent"
	// ErrorKindBusiness marks a routable business error emitted on the error
	// output port rather than retried as a system failure.
	ErrorKindBusiness ErrorKind = "business"
	// ErrorKindErrorPort marks an explicit error-port output with no structured
	// classification; the engine treats it as transient until max attempts.
	ErrorKindErrorPort ErrorKind = "error_port"
)

// NodeTimeoutErrorCode is the ClassifiedError.Code of the permanent error a node
// fails with when its execution deadline elapses. It is an externally visible
// wire value (persisted and sent over the Runner Protocol) and must not change.
const NodeTimeoutErrorCode = "node.timeout"

// ClassifiedError is the stable wire error DTO for the Runner Protocol. It
// preserves retry/permanent classification across the process boundary so the
// server can apply retry/on-error policy without inferring it from error text.
//
// It satisfies errors.Is(err, ErrPermanent): a ClassifiedError with Permanent
// set reports true for ErrPermanent, so existing types.IsPermanent callers
// work uniformly for in-process and wire-recovered errors.
//
// Wire compatibility: the protocol serializes ClassifiedError alongside the
// legacy string error field. Older peers that only read the string still
// function (losing classification); newer peers recover the full DTO. See
// service/protocol.MarshalTaskResult.
type ClassifiedError struct {
	Kind      ErrorKind      `json:"kind,omitempty"`
	Code      string         `json:"code,omitempty"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable,omitempty"`
	Permanent bool           `json:"permanent,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

func (e *ClassifiedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code != "" {
		return e.Code + ": " + e.Message
	}
	return e.Message
}

// Is makes errors.Is(err, ErrPermanent) true when the error is marked
// permanent, so ClassifiedError interoperates with types.IsPermanent and the
// existing ErrPermanent sentinel machinery.
func (e *ClassifiedError) Is(target error) bool {
	return target == ErrPermanent && e != nil && e.Permanent
}

// NewPermanentError returns a ClassifiedError marked permanent (not retryable).
func NewPermanentError(code, message string) *ClassifiedError {
	return &ClassifiedError{Kind: ErrorKindPermanent, Code: code, Message: message, Permanent: true}
}

// NewTransientError returns a ClassifiedError marked retryable.
func NewTransientError(code, message string) *ClassifiedError {
	return &ClassifiedError{Kind: ErrorKindTransient, Code: code, Message: message, Retryable: true}
}
