// Package kafka is the optional Kafka implementation of queue.Transport.
//
// The default task transport is ../asynq (Redis). This package is used only when
// a deployment explicitly selects Kafka; nothing in the default wiring imports
// it, and the asynq transport does not depend on it. It is the only place that
// imports github.com/segmentio/kafka-go (always under the kafkago alias, because
// the package name here would otherwise collide with the library's).
//
// The package must stay under internal/queue. The queue.Transport contract and
// the engine types it carries are internal to the distributed backend, and Go's
// internal-directory rule is what keeps a broker implementation from being
// imported by, or depending on, anything outside that backend.
package kafka
