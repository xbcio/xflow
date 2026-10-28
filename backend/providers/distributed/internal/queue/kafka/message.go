package kafka

import (
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// Kafka message header names. The task itself travels in the message value; the
// headers carry only transport bookkeeping, so a relay or a dead-letter writer
// can move a message without decoding it.
const (
	// hdrAttempt is the number of redeliveries so far, in decimal. A missing
	// header means 0, which is what a message published by the producer carries.
	hdrAttempt = "x-xflow-attempt"
	// hdrNotBefore is the earliest delivery time in Unix milliseconds. The delay
	// relay holds a message until it is due.
	hdrNotBefore = "x-xflow-not-before-ms"
	// hdrTarget is the topic the delay relay delivers a due message to.
	hdrTarget = "x-xflow-target"
	// hdrSource is the topic a dead-lettered message was consumed from.
	hdrSource = "x-xflow-source"
	// hdrError is a short description of why a message was dead-lettered.
	hdrError = "x-xflow-error"

	// maxErrorHeaderBytes caps hdrError. Headers ride on every record, and an
	// error chain can be arbitrarily long.
	maxErrorHeaderBytes = 512
)

// encodeTask builds the Kafka message for a task. The value is the same JSON
// the asynq transport enqueues (queue.MarshalWithNamespace), so a task keeps its
// scheduler metadata across the broker.
func encodeTask(topic string, task *engine.Task, ns namespace.Namespace) (kafkago.Message, error) {
	if task == nil {
		return kafkago.Message{}, errors.New("kafka: cannot encode a nil task")
	}
	value, err := queue.MarshalWithNamespace(task, ns)
	if err != nil {
		return kafkago.Message{}, err
	}
	return kafkago.Message{Topic: topic, Key: messageKey(task), Value: value}, nil
}

// messageKey picks the partition key. The default is the execution ID, so every
// task of one execution lands on one partition and is consumed in order. Batch
// tasks use ExecutionID + "/" + NodeName instead: NodeName already ends in
// "/_batch/<index>", so the batches of one expanded map spread across
// partitions rather than queueing behind each other on one. A nil task has no
// key, and neither does a non-batch task without an execution ID: it belongs to
// no execution whose order matters, and an empty (non-nil) key would hash every
// such task onto one partition, where a nil key lets the producer spread them.
func messageKey(task *engine.Task) []byte {
	if task == nil {
		return nil
	}
	if task.Type == engine.TaskTypeNodeBatch {
		return []byte(string(task.ExecutionID) + "/" + task.NodeName)
	}
	if task.ExecutionID == "" {
		return nil
	}
	return []byte(task.ExecutionID)
}

// decodeTask restores the task and namespace carried in a message value.
func decodeTask(msg kafkago.Message) (*engine.Task, namespace.Namespace, error) {
	return queue.Unmarshal(msg.Value)
}

// headerValue returns the value of the first header named key.
func headerValue(msg kafkago.Message, key string) (string, bool) {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// setHeader returns h with key set to val. An existing header of that key is
// replaced at its position and further duplicates are dropped, so a key never
// appears twice. The result is always a fresh slice; the argument's backing
// array is not written to, because a consumed message's headers must stay
// intact for the retry path that still holds the original.
func setHeader(h []kafkago.Header, key, val string) []kafkago.Header {
	out := make([]kafkago.Header, 0, len(h)+1)
	replaced := false
	for _, e := range h {
		if e.Key != key {
			out = append(out, e)
			continue
		}
		if !replaced {
			out = append(out, kafkago.Header{Key: key, Value: []byte(val)})
			replaced = true
		}
	}
	if !replaced {
		out = append(out, kafkago.Header{Key: key, Value: []byte(val)})
	}
	return out
}

// attemptOf reads the redelivery count. A missing, unparsable or negative value
// is 0: a message nobody stamped is a first delivery.
func attemptOf(msg kafkago.Message) int {
	v, ok := headerValue(msg, hdrAttempt)
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// notBeforeOf reads the earliest delivery time. It reports false when the
// header is missing or unparsable, or when it is not a positive Unix
// millisecond value; the caller then treats the message as due.
func notBeforeOf(msg kafkago.Message) (time.Time, bool) {
	v, ok := headerValue(msg, hdrNotBefore)
	if !ok {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// truncateUTF8 shortens s to at most max bytes without cutting through a
// multi-byte character.
func truncateUTF8(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	n := max
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
