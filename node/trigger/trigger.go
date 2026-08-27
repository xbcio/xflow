// Package trigger is the entry point for xflow's built-in trigger nodes. Each
// trigger kind lives in its own subpackage (timer, cron, webhook, kafka,
// redis); this package holds only the factory functions and the
// custom-trigger definition forwarders.
//
// The factories return the subpackages' concrete *Node types rather than an
// interface, so importing this package is what links the subpackages in — and
// what runs their init() self-registration against node/registry. That makes
// the imports below real uses, not blank imports that a cleanup could delete.
//
// Nothing here may be imported BY a subpackage: this package imports all five,
// so the reverse edge would be an import cycle. That is why kafka's Observer
// lives in node/trigger/kafka, not here.
package trigger

import (
	core "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/trigger/cron"
	"github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/node/trigger/redis"
	"github.com/xbcio/xflow/node/trigger/timer"
	"github.com/xbcio/xflow/node/trigger/webhook"
)

// ActivateFunc starts a subscription for a custom trigger. It is called once
// per activation; the returned TriggerSubscription is closed on deactivation.
type ActivateFunc = core.TriggerActivateFunc

// Definition is a custom trigger built with Define.
type Definition = core.TriggerDefinition

// Define declares a custom trigger node type backed by activate. Panics if
// nodeType is empty or activate is nil — a trigger that cannot activate is a
// wiring error, not a runtime condition.
func Define(nodeType string, activate ActivateFunc) *Definition {
	return core.DefineTrigger(nodeType, activate)
}

// Timer creates a trigger that fires on a fixed interval.
func Timer() *timer.Node { return timer.New() }

// Cron creates a trigger that fires on a cron expression.
func Cron() *cron.Node { return cron.New() }

// Webhook creates a trigger that fires on an inbound HTTP request.
func Webhook() *webhook.Node { return webhook.New() }

// Kafka creates a trigger that consumes a Kafka topic. Aggregation, message
// schema validation and dead-lettering are configured through its builder
// methods; the parameter types (kafka.AggregateConfig, kafka.MessageSchema)
// live in the kafka subpackage.
func Kafka() *kafka.Node { return kafka.New() }

// Redis creates a trigger that consumes a Redis stream or pub/sub channel.
func Redis() *redis.Node { return redis.New() }
