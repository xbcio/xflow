package kafka

import (
	"fmt"
	"strings"

	"github.com/xbcio/xflow/engine"
)

// Kafka topic names. Every name carries the "xflow." prefix, mirroring the
// "xflow:" prefix of the asynq queue names: a Kafka cluster is commonly shared,
// and a bare "default" or "task" topic would be every application's topic on it.
//
// Four topics exist per deployment:
//
//   - default: ordinary node tasks.
//   - batch: batch continuations (an expanded map's items). They get their own
//     topic for the same reason they get their own asynq queue: a wide map must
//     not sit ahead of an unrelated execution's first node.
//   - delay: tasks waiting for their due time before they move to default/batch.
//   - dlq: tasks the consumer gave up on.
const (
	topicPrefix = "xflow"

	topicDefaultBase = "task.default"
	topicBatchBase   = "task.batch"
	topicDelayBase   = "task.delay"
	topicDLQBase     = "task.dlq"

	// maxTopicLen is Kafka's limit on a topic name (TopicConstraints.MAX_NAME_LENGTH).
	maxTopicLen = 249
)

// topicNames resolves the Kafka topic names one deployment uses. The suffix is
// the deployment identity derived from the owned namespace set
// (namespace.DeploymentSuffix): empty for the default deployment, which keeps
// the unsuffixed names, and the single dedicated namespace otherwise. A
// dedicated deployment therefore reads and writes topics no other deployment
// touches.
type topicNames struct{ prefix string }

// newTopicNames builds names from namespace.DeploymentSuffix. An empty suffix
// omits the segment.
//
// The suffix is validated, never rewritten. Replacing a disallowed character
// would map two different namespaces onto one topic and silently merge their
// task streams, so an unusable suffix is an error that names it.
func newTopicNames(suffix string) (topicNames, error) {
	prefix := topicPrefix + "."
	if suffix != "" {
		if err := validateSuffix(suffix); err != nil {
			return topicNames{}, err
		}
		prefix += suffix + "."
	}
	n := topicNames{prefix: prefix}
	for _, topic := range n.All() {
		if len(topic) > maxTopicLen {
			return topicNames{}, fmt.Errorf("kafka topic %q (%d bytes) exceeds the %d-byte topic name limit; "+
				"namespace suffix %q is too long", topic, len(topic), maxTopicLen, suffix)
		}
	}
	// A group ID shares the 249-byte cap. "consumer.default" is longer than any
	// topic base, so a suffix that fits every topic can still overflow here.
	for _, kind := range []string{groupDefault, groupBatch, groupDelay} {
		if g := n.Group(kind); len(g) > maxTopicLen {
			return topicNames{}, fmt.Errorf("kafka consumer group %q (%d bytes) exceeds the %d-byte limit; "+
				"namespace suffix %q is too long", g, len(g), maxTopicLen, suffix)
		}
	}
	return n, nil
}

// validateSuffix rejects a suffix Kafka would not accept inside a topic name.
// Topic names allow only [A-Za-z0-9._-]; "." and ".." are reserved names, and
// a suffix of either would also produce an empty name segment.
func validateSuffix(suffix string) error {
	if suffix == "." || suffix == ".." {
		return fmt.Errorf("namespace suffix %q is not usable in a kafka topic name", suffix)
	}
	for _, r := range suffix {
		if !topicChar(r) {
			return fmt.Errorf("namespace suffix %q contains %q, which is not allowed in a kafka topic name "+
				"(allowed: A-Z a-z 0-9 . _ -)", suffix, r)
		}
	}
	return nil
}

// topicChar reports whether r is in Kafka's topic name alphabet.
func topicChar(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return r == '.' || r == '_' || r == '-'
}

// name appends a base topic name to the deployment prefix.
func (n topicNames) name(base string) string { return n.prefix + base }

// Default is the topic for ordinary node tasks.
func (n topicNames) Default() string { return n.name(topicDefaultBase) }

// Batch is the topic for batch continuations.
func (n topicNames) Batch() string { return n.name(topicBatchBase) }

// Delay is the topic that holds delayed tasks until they are due.
func (n topicNames) Delay() string { return n.name(topicDelayBase) }

// DLQ is the topic for tasks the consumer gave up on.
func (n topicNames) DLQ() string { return n.name(topicDLQBase) }

// For routes a task to its topic. A nil task goes to the default topic.
func (n topicNames) For(t *engine.Task) string {
	if t != nil && t.Type == engine.TaskTypeNodeBatch {
		return n.Batch()
	}
	return n.Default()
}

// All lists every topic this deployment uses, in a stable order: default,
// batch, delay, dlq.
func (n topicNames) All() []string {
	return []string{n.Default(), n.Batch(), n.Delay(), n.DLQ()}
}

// Consumer group kinds. Each consumed topic has its own group, so default and
// batch commit independently and each is balanced across the runners on its own.
const (
	groupDefault = "default"
	groupBatch   = "batch"
	groupDelay   = "delay"

	groupInfix = "consumer."
)

// Group is the consumer group ID for kind (groupDefault, groupBatch or
// groupDelay): "xflow.<suffix>.consumer.<kind>", with the suffix segment omitted
// for the default deployment. The suffix is already validated by
// newTopicNames and the kinds are fixed, so the name is always a legal group
// ID; the length cap is checked there too.
func (n topicNames) Group(kind string) string { return n.prefix + groupInfix + kind }

// suffixCollisionRisk reports whether suffix contains '.' or '_'. Kafka folds
// the two into the same character in metric names, so topics that differ only
// in those characters ("a.b" and "a_b") share metrics even though they are
// distinct topics. The names are still valid; the caller logs a warning.
func suffixCollisionRisk(suffix string) bool {
	return strings.ContainsAny(suffix, "._")
}
