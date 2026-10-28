package kafka

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
)

// errKind is how the producer treats a failed produce.
type errKind int

const (
	// kindRetryable is transient broker or network trouble. The producer reports
	// backpressure and the outbox keeps the entry for the next attempt.
	kindRetryable errKind = iota
	// kindPermanent is a failure retrying cannot fix (configuration, auth,
	// message size). The entry is dead-lettered instead of retried forever.
	kindPermanent
	// kindCaller means the caller's own context ended. It says nothing about the
	// broker and must not count as a broker fault.
	kindCaller
)

// classify decides how a produce error is treated. ctx is the caller's context.
//
// The bias is deliberate: an error that is not recognised is retryable. Wrongly
// calling a transient error retryable only keeps the outbox backed up, which
// alerts and loses nothing. Wrongly calling it permanent pushes a good entry
// into the dead-letter queue.
//
// Transient network errors (net.Error including timeouts, io.EOF,
// io.ErrUnexpectedEOF, ECONNREFUSED, ECONNRESET) need no branch of their own:
// they fall through to the retryable default. The closed-resource errors that
// also implement net.Error are matched earlier, so they stay permanent.
func classify(ctx context.Context, err error) errKind {
	if err == nil {
		return kindRetryable
	}
	if cerr := ctx.Err(); cerr != nil &&
		(errors.Is(err, cerr) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return kindCaller
	}
	if werrs, ok := errors.AsType[kafkago.WriteErrors](err); ok {
		return classifyWrite(ctx, werrs)
	}
	if kerr, ok := errors.AsType[kafkago.Error](err); ok {
		return classifyCode(kerr)
	}
	if errors.Is(err, kafkago.ErrGroupClosed) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) {
		return kindPermanent
	}
	return kindRetryable
}

// classifyWrite folds the per-message errors of one WriteMessages call into a
// single verdict. Nil entries are messages that were written and are skipped.
// Any permanent entry makes the whole call permanent, because retrying the batch
// would resend that entry and fail the same way. Otherwise an entry that failed
// because the caller's context ended makes it a caller error. Only a call whose
// every failed entry is retryable is retryable, and an empty one counts as
// retryable.
func classifyWrite(ctx context.Context, werrs kafkago.WriteErrors) errKind {
	verdict := kindRetryable
	for _, e := range werrs {
		if e == nil {
			continue
		}
		switch classify(ctx, e) {
		case kindPermanent:
			return kindPermanent
		case kindCaller:
			verdict = kindCaller
		}
	}
	return verdict
}

// permanentCodes are the broker error codes that retrying cannot fix: the
// request, the configuration or the credentials are wrong, or the message is
// too big. Every code not listed is retryable.
//
// The list is explicit, and deliberately not "anything kafka-go does not mark
// Temporary()". That rule would call BrokerNotAvailable, ReplicaNotAvailable and
// UnknownServerError permanent, although a restarting broker or a transient
// server fault produces exactly those, and an unrecognised error is meant to be
// retryable (see classify).
//
// UnknownTopicOrPartition is on the list even though kafka-go marks it
// temporary: for this transport a missing topic is a deployment error. The
// window right after a topic is created is not covered by retrying this error;
// the startup check that the topics exist (EnsureTopics) is what covers it.
var permanentCodes = map[kafkago.Error]struct{}{
	kafkago.MessageSizeTooLarge:         {},
	kafkago.TopicAuthorizationFailed:    {},
	kafkago.GroupAuthorizationFailed:    {},
	kafkago.SASLAuthenticationFailed:    {},
	kafkago.InvalidTopic:                {},
	kafkago.UnknownTopicOrPartition:     {},
	kafkago.InvalidRequest:              {},
	kafkago.UnsupportedVersion:          {},
	kafkago.UnsupportedForMessageFormat: {},
	kafkago.ClusterAuthorizationFailed:  {},
	kafkago.InvalidConfiguration:        {},
	kafkago.PolicyViolation:             {},
	kafkago.InvalidRecord:               {},
	kafkago.RecordListTooLarge:          {},
	kafkago.InvalidRequiredAcks:         {},
	kafkago.UnsupportedSASLMechanism:    {},
	kafkago.IllegalSASLState:            {},
	kafkago.InvalidPartitionNumber:      {},
	kafkago.InvalidReplicationFactor:    {},
}

// The kinds a failed enqueue is reported under to a queue.ProduceErrorObserver.
// The set is fixed and small on purpose: a metrics label built from it has a
// bounded number of values, and none of them ever comes from error text.
const (
	// ProduceKindQueueFull is broker trouble that retrying can fix, reported to
	// the caller as engine.ErrQueueFull (TryEnqueue and EnqueueDelayed). The
	// outbox keeps the entry and tries again.
	ProduceKindQueueFull = "queue_full"
	// ProduceKindRetryable is the same trouble on the direct Enqueue path, which
	// does not map it: the caller sees the broker's own error and no outbox entry
	// stands behind the task.
	ProduceKindRetryable = "retryable"
	// ProduceKindPermanent is a failure retrying cannot fix: configuration,
	// credentials, a missing topic or an oversized message.
	ProduceKindPermanent = "permanent"
	// ProduceKindCaller is the caller's own context ending. It is no broker fault
	// and is kept apart so an alert on broker trouble can leave it out.
	ProduceKindCaller = "caller"
	// ProduceKindOther is everything that never reached the broker: a task that
	// cannot be encoded, or a delay over Config.MaxEnqueueDelay.
	ProduceKindOther = "other"
)

// ProduceKinds lists every kind the transport reports, so the adapter that turns
// them into a metric label can be checked against it.
func ProduceKinds() []string {
	return []string{ProduceKindQueueFull, ProduceKindRetryable, ProduceKindPermanent, ProduceKindCaller, ProduceKindOther}
}

// produceKind names the kind of a failed produce. err is what the caller gets
// back, so a retryable failure that was mapped to engine.ErrQueueFull reads as
// queue_full while one that was not mapped stays retryable.
func produceKind(ctx context.Context, err error) string {
	if errors.Is(err, engine.ErrQueueFull) {
		return ProduceKindQueueFull
	}
	switch classify(ctx, err) {
	case kindPermanent:
		return ProduceKindPermanent
	case kindCaller:
		return ProduceKindCaller
	default:
		return ProduceKindRetryable
	}
}

// classifyCode classifies a broker error code: permanent if it is in
// permanentCodes, retryable otherwise.
func classifyCode(e kafkago.Error) errKind {
	if _, ok := permanentCodes[e]; ok {
		return kindPermanent
	}
	return kindRetryable
}

// isMessageTooLarge reports whether err says a message exceeds a size limit:
// the writer's own check (kafkago.MessageTooLargeError), the broker's
// MessageSizeTooLarge code, or either of them as an entry of a WriteErrors.
// kafka-go gives WriteErrors no Unwrap, so its entries are visited here.
func isMessageTooLarge(err error) bool {
	if err == nil {
		return false
	}
	if werrs, ok := errors.AsType[kafkago.WriteErrors](err); ok {
		return slices.ContainsFunc(werrs, isMessageTooLarge)
	}
	if _, ok := errors.AsType[kafkago.MessageTooLargeError](err); ok {
		return true
	}
	return errors.Is(err, kafkago.MessageSizeTooLarge)
}
