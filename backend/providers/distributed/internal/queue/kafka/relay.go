package kafka

import (
	"context"
	"errors"
	"io"
	"slices"

	kafkago "github.com/segmentio/kafka-go"
)

// errNoRelayTarget is the dead-letter reason of a delay message that does not
// say where it goes.
const errNoRelayTarget = "delay message has no valid target"

// runRelay moves due messages from the delay topic back to the topic they are
// destined for. It returns when ctx ends or the reader is closed.
//
// The delay topic holds two kinds of message: tasks the producer scheduled for
// later (EnqueueDelayed) and tasks a worker parked for a retry. Both carry
// hdrNotBefore and hdrTarget.
//
// Messages are handled strictly one at a time, in the order they were produced.
// The topic is ordered by arrival, not by due time, so a message that is due
// later holds back every message behind it in its partition, even ones due
// sooner. This head-of-line blocking is accepted for v1: this is the cold path
// (the outbox already holds back tasks that are not due), and retry delays are
// capped at Config.MaxRetryBackoff, so a message waits at most that long behind
// another. Waiting costs no group membership: kafka-go heartbeats from its own
// goroutine, independent of this loop.
//
// A message is committed only after its forwarded copy is written. A crash in
// between forwards it twice; the duplicate is absorbed by lease fencing like
// any other redelivery.
func (t *Transport) runRelay(ctx context.Context, reader messageReader, cm *committer) {
	pause := retryPauseMin
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			t.warn("kafka relay fetch failed", "topic", t.names.Delay(), "error", err)
			if !t.pause(ctx, pause) {
				return
			}
			pause = min(pause*2, retryPauseMax)
			continue
		}
		pause = retryPauseMin
		if !t.relayOne(ctx, msg) {
			return
		}
		// One message at a time means no earlier message is still open, so its
		// own offset is the watermark and the tracker is not needed.
		cm.Advance(msg.Partition, msg.Offset)
	}
}

// relayOne forwards a single delay message once it is due, or dead-letters it if
// it has no usable target. It reports false only when ctx ended before the
// message was written anywhere; the message then stays uncommitted.
func (t *Transport) relayOne(ctx context.Context, msg kafkago.Message) bool {
	target, ok := headerValue(msg, hdrTarget)
	if !ok || (target != t.names.Default() && target != t.names.Batch()) {
		// Checked before waiting: a message that can never be delivered must not
		// hold the partition until its due time.
		return t.deadLetterMessage(ctx, msg, t.names.Delay(), errNoRelayTarget)
	}
	if due, ok := notBeforeOf(msg); ok && !t.waitUntil(ctx, due) {
		return false
	}
	// All headers travel along, including hdrAttempt, so the attempt count
	// survives the trip. hdrNotBefore and hdrTarget are stale on the target
	// topic but harmless: a retry overwrites both with setHeader.
	return t.publishCopy(ctx, kafkago.Message{
		Topic:   target,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: slices.Clone(msg.Headers),
	}, msg, t.names.Delay(), "forwarded copy too large")
}
