package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// timeoutNetErr is a net.Error whose Timeout result is configurable.
type timeoutNetErr struct{ timeout bool }

func (e timeoutNetErr) Error() string   { return "fake net error" }
func (e timeoutNetErr) Timeout() bool   { return e.timeout }
func (e timeoutNetErr) Temporary() bool { return false }

var _ net.Error = timeoutNetErr{}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredCtx() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	cancel()
	return ctx
}

func TestClassify_Nil(t *testing.T) {
	if got := classify(context.Background(), nil); got != kindRetryable {
		t.Fatalf("classify(nil) = %v, want kindRetryable", got)
	}
	if got := classify(canceledCtx(), nil); got != kindRetryable {
		t.Fatalf("classify(canceled ctx, nil) = %v, want kindRetryable", got)
	}
}

func TestClassify_CallerContext(t *testing.T) {
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want errKind
	}{
		{"canceled", canceledCtx(), context.Canceled, kindCaller},
		{"canceled wrapped", canceledCtx(), fmt.Errorf("produce: %w", context.Canceled), kindCaller},
		{"deadline", expiredCtx(), context.DeadlineExceeded, kindCaller},
		{"deadline wrapped", expiredCtx(), fmt.Errorf("produce: %w", context.DeadlineExceeded), kindCaller},
		// The writer can surface a context error that is not the caller's own
		// ctx.Err() value; a live caller context still means the caller ended it.
		{"canceled error but live ctx", context.Background(), context.Canceled, kindRetryable},
		{"deadline error but live ctx", context.Background(), context.DeadlineExceeded, kindRetryable},
		// A permanent broker error stays permanent even when the caller has
		// since been canceled: it is not a context error.
		{"permanent error under canceled ctx", canceledCtx(), kafkago.MessageSizeTooLarge, kindPermanent},
		{"retryable error under canceled ctx", canceledCtx(), kafkago.LeaderNotAvailable, kindRetryable},
	}
	for _, c := range cases {
		if got := classify(c.ctx, c.err); got != c.want {
			t.Errorf("%s: classify = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestClassify_BrokerErrorCodes(t *testing.T) {
	permanent := []kafkago.Error{
		kafkago.MessageSizeTooLarge,
		kafkago.TopicAuthorizationFailed,
		kafkago.GroupAuthorizationFailed,
		kafkago.SASLAuthenticationFailed,
		kafkago.InvalidTopic,
		kafkago.UnknownTopicOrPartition,
		kafkago.InvalidRequest,
		kafkago.UnsupportedVersion,
		kafkago.UnsupportedForMessageFormat,
		kafkago.ClusterAuthorizationFailed,
		kafkago.InvalidConfiguration,
		kafkago.PolicyViolation,
		kafkago.InvalidRecord,
		kafkago.RecordListTooLarge,
		kafkago.InvalidRequiredAcks,
		kafkago.UnsupportedSASLMechanism,
		kafkago.IllegalSASLState,
		kafkago.InvalidPartitionNumber,
		kafkago.InvalidReplicationFactor,
	}
	for _, e := range permanent {
		if got := classify(context.Background(), e); got != kindPermanent {
			t.Errorf("classify(%v) = %v, want kindPermanent", e, got)
		}
		wrapped := fmt.Errorf("write topic: %w", e)
		if got := classify(context.Background(), wrapped); got != kindPermanent {
			t.Errorf("classify(wrapped %v) = %v, want kindPermanent", e, got)
		}
	}

	retryable := []kafkago.Error{
		kafkago.LeaderNotAvailable,
		kafkago.NotLeaderForPartition,
		kafkago.RequestTimedOut,
		kafkago.NetworkException,
		kafkago.NotEnoughReplicas,
		kafkago.NotEnoughReplicasAfterAppend,
		kafkago.KafkaStorageError,
		kafkago.NotController,
	}
	for _, e := range retryable {
		if !e.Temporary() {
			t.Fatalf("test premise broken: %v is no longer Temporary()", e)
		}
		if got := classify(context.Background(), e); got != kindRetryable {
			t.Errorf("classify(%v) = %v, want kindRetryable", e, got)
		}
	}

	// A code that is not on the explicit permanent list is retryable, whether or
	// not kafka-go marks it temporary. Marking is not the test: these three are
	// what a restarting broker or a transient server fault returns.
	for _, e := range []kafkago.Error{
		kafkago.BrokerNotAvailable,
		kafkago.ReplicaNotAvailable,
		kafkago.Unknown,
		kafkago.OffsetOutOfRange,
	} {
		if got := classify(context.Background(), e); got != kindRetryable {
			t.Errorf("classify(%v) = %v, want kindRetryable", e, got)
		}
		wrapped := fmt.Errorf("write topic: %w", e)
		if got := classify(context.Background(), wrapped); got != kindRetryable {
			t.Errorf("classify(wrapped %v) = %v, want kindRetryable", e, got)
		}
	}
}

// UnknownTopicOrPartition is the case the explicit list exists for: kafka-go
// marks it temporary, and the classifier must not follow it.
func TestClassify_UnknownTopicOrPartitionIgnoresTemporary(t *testing.T) {
	if !kafkago.UnknownTopicOrPartition.Temporary() {
		t.Fatal("test premise broken: kafka-go no longer marks UnknownTopicOrPartition temporary")
	}
	if got := classify(context.Background(), kafkago.UnknownTopicOrPartition); got != kindPermanent {
		t.Fatalf("classify(UnknownTopicOrPartition) = %v, want kindPermanent", got)
	}
}

func TestClassify_MessageTooLargeError(t *testing.T) {
	err := kafkago.MessageTooLargeError{}
	if got := classify(context.Background(), err); got != kindPermanent {
		t.Fatalf("classify(MessageTooLargeError) = %v, want kindPermanent", got)
	}
}

func TestClassify_WriteErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want errKind
	}{
		{"empty", kafkago.WriteErrors{}, kindRetryable},
		{"all nil", kafkago.WriteErrors{nil, nil}, kindRetryable},
		{"single retryable", kafkago.WriteErrors{kafkago.LeaderNotAvailable}, kindRetryable},
		{"all retryable", kafkago.WriteErrors{kafkago.LeaderNotAvailable, kafkago.RequestTimedOut, io.EOF}, kindRetryable},
		{"retryable with nil entries", kafkago.WriteErrors{nil, kafkago.NotLeaderForPartition, nil}, kindRetryable},
		{"single permanent", kafkago.WriteErrors{kafkago.MessageSizeTooLarge}, kindPermanent},
		{"mixed retryable then permanent", kafkago.WriteErrors{kafkago.LeaderNotAvailable, kafkago.TopicAuthorizationFailed}, kindPermanent},
		{"mixed permanent then retryable", kafkago.WriteErrors{kafkago.InvalidTopic, kafkago.RequestTimedOut, nil}, kindPermanent},
		{"unrecognized entry is retryable", kafkago.WriteErrors{errors.New("???")}, kindRetryable},
		{"wrapped all retryable", fmt.Errorf("batch: %w", kafkago.WriteErrors{kafkago.LeaderNotAvailable}), kindRetryable},
		{"wrapped mixed", fmt.Errorf("batch: %w", kafkago.WriteErrors{kafkago.LeaderNotAvailable, kafkago.UnknownTopicOrPartition}), kindPermanent},
		{"closed pipe entry", kafkago.WriteErrors{io.ErrClosedPipe, kafkago.LeaderNotAvailable}, kindPermanent},
		{"nested write errors", kafkago.WriteErrors{kafkago.WriteErrors{kafkago.SASLAuthenticationFailed}}, kindPermanent},
	}
	for _, c := range cases {
		if got := classify(context.Background(), c.err); got != c.want {
			t.Errorf("%s: classify = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestClassify_WriteErrorsCallerContext(t *testing.T) {
	ctx := canceledCtx()
	if got := classify(ctx, kafkago.WriteErrors{context.Canceled, context.Canceled}); got != kindCaller {
		t.Errorf("all canceled entries: classify = %v, want kindCaller", got)
	}
	if got := classify(ctx, kafkago.WriteErrors{nil, context.Canceled}); got != kindCaller {
		t.Errorf("canceled entry among nil: classify = %v, want kindCaller", got)
	}
	if got := classify(ctx, kafkago.WriteErrors{kafkago.LeaderNotAvailable, context.Canceled}); got != kindCaller {
		t.Errorf("canceled entry with retryable: classify = %v, want kindCaller", got)
	}
	// A permanent entry outranks the caller's cancellation: the entry would
	// fail the same way on any later attempt.
	if got := classify(ctx, kafkago.WriteErrors{context.Canceled, kafkago.MessageSizeTooLarge}); got != kindPermanent {
		t.Errorf("canceled entry with permanent: classify = %v, want kindPermanent", got)
	}
}

func TestClassify_ClosedWriter(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"ErrGroupClosed", kafkago.ErrGroupClosed},
		{"ErrClosedPipe", io.ErrClosedPipe},
		{"net.ErrClosed", net.ErrClosed},
		{"wrapped ErrClosedPipe", fmt.Errorf("write: %w", io.ErrClosedPipe)},
		{"wrapped net.ErrClosed", fmt.Errorf("write: %w", net.ErrClosed)},
		{"OpError over net.ErrClosed", &net.OpError{Op: "write", Err: net.ErrClosed}},
	}
	for _, c := range cases {
		if got := classify(context.Background(), c.err); got != kindPermanent {
			t.Errorf("%s: classify = %v, want kindPermanent", c.name, got)
		}
	}
	// os.ErrClosed is a different sentinel and is not treated as a writer close.
	if got := classify(context.Background(), os.ErrClosed); got != kindRetryable {
		t.Errorf("os.ErrClosed: classify = %v, want kindRetryable (unrecognized)", got)
	}
}

func TestClassify_NetworkErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"net.Error timeout", timeoutNetErr{timeout: true}},
		{"net.Error non-timeout", timeoutNetErr{timeout: false}},
		{"OpError timeout", &net.OpError{Op: "dial", Err: timeoutNetErr{timeout: true}}},
		{"io.EOF", io.EOF},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF},
		{"ECONNREFUSED", syscall.ECONNREFUSED},
		{"ECONNRESET", syscall.ECONNRESET},
		{"wrapped ECONNREFUSED", fmt.Errorf("dial: %w", syscall.ECONNREFUSED)},
		{"OpError over ECONNRESET", &net.OpError{Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}},
		{"wrapped EOF", fmt.Errorf("read: %w", io.EOF)},
	}
	for _, c := range cases {
		if got := classify(context.Background(), c.err); got != kindRetryable {
			t.Errorf("%s: classify = %v, want kindRetryable", c.name, got)
		}
	}
}

func TestClassify_UnrecognizedIsRetryable(t *testing.T) {
	cases := []error{
		errors.New("something nobody anticipated"),
		fmt.Errorf("wrapped: %w", errors.New("opaque")),
		errors.Join(errors.New("a"), errors.New("b")),
	}
	for _, err := range cases {
		if got := classify(context.Background(), err); got != kindRetryable {
			t.Errorf("classify(%v) = %v, want kindRetryable", err, got)
		}
	}
}

// A broker error code outranks a transport-level wrapper around it.
func TestClassify_BrokerCodeOutranksNetworkWrapper(t *testing.T) {
	err := &net.OpError{Op: "write", Err: kafkago.TopicAuthorizationFailed}
	if got := classify(context.Background(), err); got != kindPermanent {
		t.Fatalf("classify(OpError over TopicAuthorizationFailed) = %v, want kindPermanent", got)
	}
}

func TestIsMessageTooLarge(t *testing.T) {
	tooLarge := kafkago.MessageTooLargeError{Message: kafkago.Message{Value: []byte("x")}}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"writer check", tooLarge, true},
		{"writer check wrapped", fmt.Errorf("produce: %w", tooLarge), true},
		{"broker code", kafkago.MessageSizeTooLarge, true},
		{"broker code wrapped", fmt.Errorf("produce: %w", kafkago.MessageSizeTooLarge), true},
		{"write errors entry", kafkago.WriteErrors{nil, kafkago.MessageSizeTooLarge}, true},
		{"write errors writer check", kafkago.WriteErrors{tooLarge}, true},
		{"write errors wrapped", fmt.Errorf("produce: %w", kafkago.WriteErrors{nil, tooLarge}), true},
		{"write errors mixed", kafkago.WriteErrors{kafkago.LeaderNotAvailable, kafkago.MessageSizeTooLarge}, true},
		{"write errors other", kafkago.WriteErrors{nil, kafkago.LeaderNotAvailable}, false},
		{"write errors empty", kafkago.WriteErrors{}, false},
		{"other broker code", kafkago.UnknownTopicOrPartition, false},
		{"auth", kafkago.TopicAuthorizationFailed, false},
		{"network", timeoutNetErr{timeout: true}, false},
		{"plain", errors.New("boom"), false},
	}
	for _, c := range cases {
		if got := isMessageTooLarge(c.err); got != c.want {
			t.Errorf("%s: isMessageTooLarge = %v, want %v", c.name, got, c.want)
		}
	}
}
