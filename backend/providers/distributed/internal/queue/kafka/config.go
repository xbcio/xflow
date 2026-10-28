package kafka

import (
	"errors"
	"fmt"
	"math"
	"net"
	"time"
)

// batchBytes is the writer's batch limit with the default applied.
func (c Config) batchBytes() int64 {
	if c.BatchBytes <= 0 {
		return defaultBatchBytes
	}
	return c.BatchBytes
}

// writeTimeout is the produce and admin request timeout with the default applied.
func (c Config) writeTimeout() time.Duration {
	if c.WriteTimeout <= 0 {
		return defaultWriteTimeout
	}
	return c.WriteTimeout
}

// maxEnqueueDelay is the delay cap EnqueueDelayed enforces, with the default
// applied to the zero value. Validate rejects a negative stored value, so a
// built Transport never runs with one.
func (c Config) maxEnqueueDelay() time.Duration {
	if c.MaxEnqueueDelay == 0 {
		return defaultMaxEnqueueDelay
	}
	return c.MaxEnqueueDelay
}

// Validate reports a Config the transport cannot run with, so a bad setting
// fails when the transport is built instead of at the first produce or fetch.
// New calls it.
//
// A field documented as "zero or negative selects the default" is never an
// error. The relations between such fields are checked against the values the
// defaults give, because the defaults are what the transport would run with.
func (c Config) Validate() error {
	if err := c.validateBrokers(); err != nil {
		return err
	}
	if _, err := buildSASLMechanism(c.SASLMechanism, c.SASLUsername, c.SASLPassword); err != nil {
		return err
	}
	d := c.withConsumerDefaults()
	// Kafka's guidance is a heartbeat of at most a third of the session timeout,
	// so that a few lost heartbeats do not evict a healthy member.
	if d.HeartbeatInterval > d.SessionTimeout/3 {
		return fmt.Errorf("kafka: HeartbeatInterval %v must not exceed SessionTimeout/3 (SessionTimeout is %v)",
			d.HeartbeatInterval, d.SessionTimeout)
	}
	// kafka-go documents that the broker truncates a record to fit MaxBytes. The
	// reader treats the truncated read as the end of the batch and fetches the
	// same offset again, with no error, so a record the writer accepts but the
	// fetch limit cannot hold would stall its partition silently. Brokers that
	// return an oversized first batch whole would not stall; the rule keeps the
	// limits consistent without relying on that.
	if batch := c.batchBytes(); d.FetchMaxBytes < batch {
		return fmt.Errorf("kafka: FetchMaxBytes %d is below BatchBytes %d, so the consumer could not fetch the largest record the producer accepts",
			d.FetchMaxBytes, batch)
	}
	if err := c.validateMaxEnqueueDelay(); err != nil {
		return err
	}
	return c.validateTopicProvisioning()
}

// validateMaxEnqueueDelay bounds the delay cap against what the engine actually
// produces and what the delay relay can absorb. The floor is the engine's own
// worst-case retry delay: EnqueueDelayed under it would reject the engine's
// retries, turning a guard meant for future callers into a live defect. The
// ceiling exists because the cap's whole purpose is bounding the relay's
// head-of-line wait; a cap far past the engine's needs trades that protection
// away silently, so an operator who wants it must ask for it inside a range we
// still consider a guard.
func (c Config) validateMaxEnqueueDelay() error {
	if c.MaxEnqueueDelay < 0 {
		return fmt.Errorf("kafka: MaxEnqueueDelay %v is negative; use 0 for the default %v",
			c.MaxEnqueueDelay, defaultMaxEnqueueDelay)
	}
	if limit := c.maxEnqueueDelay(); limit < minMaxEnqueueDelay {
		return fmt.Errorf("kafka: MaxEnqueueDelay %v is below %v, the engine's longest retry delay (5m backoff cap plus up to 20%% jitter); it would reject the engine's own retries",
			limit, minMaxEnqueueDelay)
	} else if limit > maxMaxEnqueueDelay {
		return fmt.Errorf("kafka: MaxEnqueueDelay %v exceeds %v; the delay relay waits in arrival order, so a longer cap admits head-of-line delays the guard exists to prevent",
			limit, maxMaxEnqueueDelay)
	}
	return nil
}

func (c Config) validateBrokers() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: Brokers: at least one broker is required")
	}
	for i, b := range c.Brokers {
		host, port, err := net.SplitHostPort(b)
		switch {
		case err != nil:
			return fmt.Errorf("kafka: Brokers[%d] = %q: %w", i, b, err)
		case host == "":
			return fmt.Errorf("kafka: Brokers[%d] = %q: host is empty", i, b)
		case port == "":
			return fmt.Errorf("kafka: Brokers[%d] = %q: port is empty", i, b)
		}
	}
	return nil
}

// validateTopicProvisioning checks CreateTopics, Partitions and
// ReplicationFactor together. The two counts have no default, so with
// CreateTopics on they must be set, and with it off they must be left unset
// instead of being silently ignored.
func (c Config) validateTopicProvisioning() error {
	if !c.CreateTopics {
		if c.Partitions != 0 {
			return fmt.Errorf("kafka: Partitions %d only takes effect when CreateTopics is true", c.Partitions)
		}
		if c.ReplicationFactor != 0 {
			return fmt.Errorf("kafka: ReplicationFactor %d only takes effect when CreateTopics is true", c.ReplicationFactor)
		}
		return nil
	}
	// The bounds are the widths of the wire fields; kafka-go would otherwise
	// truncate a larger value without a word.
	if c.Partitions <= 0 || c.Partitions > math.MaxInt32 {
		return fmt.Errorf("kafka: Partitions %d must be between 1 and %d when CreateTopics is true", c.Partitions, math.MaxInt32)
	}
	if c.ReplicationFactor <= 0 || c.ReplicationFactor > math.MaxInt16 {
		return fmt.Errorf("kafka: ReplicationFactor %d must be between 1 and %d when CreateTopics is true", c.ReplicationFactor, math.MaxInt16)
	}
	return nil
}
