package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	// ensureWindow bounds EnsureTopics when the caller's context has no deadline.
	ensureWindow = 30 * time.Second
	// ensurePollMin and ensurePollMax bound the wait between checks while a
	// topic that was just created, or one that is not fully available yet,
	// propagates through the cluster.
	ensurePollMin = 200 * time.Millisecond
	ensurePollMax = 2 * time.Second
	// adminMetadataTTL is how long the admin client's transport caches cluster
	// metadata. It is short because EnsureTopics polls that cache while waiting
	// for another runner's topic creation to become visible.
	adminMetadataTTL = time.Second
)

// topicInfo is what the admin reports about one topic.
type topicInfo struct {
	// Partitions is the topic's partition count; zero when it does not exist.
	Partitions int
	// Err is the broker's verdict: kafkago.UnknownTopicOrPartition for a topic
	// that does not exist, another code for a topic that exists but is not
	// usable yet (for example LeaderNotAvailable), nil when it is ready.
	Err error
}

// topicAdmin is the part of cluster administration EnsureTopics uses. Tests
// replace it with a fake; clientAdmin is the real one.
type topicAdmin interface {
	// Topics describes each named topic. A missing topic is a result with
	// Err set, not an error of the call.
	Topics(ctx context.Context, names []string) (map[string]topicInfo, error)
	// CreateTopics creates the topics and returns the per-topic outcome. A nil
	// entry means created.
	CreateTopics(ctx context.Context, topics []kafkago.TopicConfig) (map[string]error, error)
	// Close releases the connections the admin holds. The admin stays usable.
	Close()
}

// withTopicAdmin replaces the topic admin. It lets tests run EnsureTopics
// against a fake without a broker.
func withTopicAdmin(a topicAdmin) Option {
	return func(t *Transport) { t.admin = a }
}

// clientAdmin is the topicAdmin backed by a kafka-go Client.
//
// Describing a topic must never create it. Client.Metadata builds its request
// without AllowAutoTopicCreation, and kafka-go's transport answers a metadata
// request from the cluster layout it already holds, going back to a broker only
// when AllowAutoTopicCreation is set and a requested topic is missing from that
// layout (transport.go, connPool.roundTrip). A topic absent from the layout is
// reported as UnknownTopicOrPartition. The layout is filled by the transport's
// own refresh, which asks for all topics because MetadataTopics is unset.
// Unlike conn.ReadPartitions, which sets AllowAutoTopicCreation, this path does
// not ask the broker to create a topic.
type clientAdmin struct {
	client    *kafkago.Client
	transport *kafkago.Transport
}

func newClientAdmin(cfg Config) (*clientAdmin, error) {
	mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
	if err != nil {
		return nil, err
	}
	tr := &kafkago.Transport{TLS: cfg.TLS, SASL: mechanism, MetadataTTL: adminMetadataTTL}
	return &clientAdmin{
		transport: tr,
		client: &kafkago.Client{
			Addr:      kafkago.TCP(cfg.Brokers...),
			Timeout:   cfg.writeTimeout(),
			Transport: tr,
		},
	}, nil
}

func (a *clientAdmin) Topics(ctx context.Context, names []string) (map[string]topicInfo, error) {
	res, err := a.client.Metadata(ctx, &kafkago.MetadataRequest{Topics: names})
	if err != nil {
		return nil, err
	}
	out := make(map[string]topicInfo, len(res.Topics))
	for _, t := range res.Topics {
		info := topicInfo{Partitions: len(t.Partitions), Err: t.Error}
		if info.Err == nil {
			// A partition without a leader is not usable yet either.
			for _, p := range t.Partitions {
				if p.Error != nil {
					info.Err = p.Error
					break
				}
			}
		}
		out[t.Name] = info
	}
	return out, nil
}

func (a *clientAdmin) CreateTopics(ctx context.Context, topics []kafkago.TopicConfig) (map[string]error, error) {
	res, err := a.client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{Topics: topics})
	if err != nil {
		return nil, err
	}
	return res.Errors, nil
}

func (a *clientAdmin) Close() { a.transport.CloseIdleConnections() }

// topicRefusedError is a topic the broker will not describe for a reason that
// waiting cannot fix, such as missing authorisation.
type topicRefusedError struct {
	topic string
	err   error
}

func (e *topicRefusedError) Error() string { return fmt.Sprintf("kafka: topic %q: %v", e.topic, e.err) }
func (e *topicRefusedError) Unwrap() error { return e.err }

// survey is the state of the deployment's topics at one moment.
type survey struct {
	infos   map[string]topicInfo
	missing []string // do not exist; sorted
	pending []string // exist but are not usable yet, such as no leader; sorted
}

func (s survey) ready() bool { return len(s.missing) == 0 && len(s.pending) == 0 }

// reasons describes every topic that is not ready, for an error message.
func (s survey) reasons() string {
	parts := make([]string, 0, len(s.missing)+len(s.pending))
	for _, name := range s.missing {
		parts = append(parts, name+": does not exist")
	}
	for _, name := range s.pending {
		parts = append(parts, fmt.Sprintf("%s: %v", name, s.infos[name].Err))
	}
	return strings.Join(parts, "; ")
}

// survey describes names. It never creates anything. A topic refused with a
// permanent broker code fails it with a *topicRefusedError; any other error
// comes from the describe call itself.
func (t *Transport) survey(ctx context.Context, names []string) (survey, error) {
	infos, err := t.admin.Topics(ctx, names)
	if err != nil {
		return survey{}, fmt.Errorf("kafka: describe topics: %w", err)
	}
	s := survey{infos: infos}
	for _, name := range names {
		info, ok := infos[name]
		switch {
		case !ok || errors.Is(info.Err, kafkago.UnknownTopicOrPartition):
			s.missing = append(s.missing, name)
		case info.Err == nil:
		default:
			if kerr, ok := errors.AsType[kafkago.Error](info.Err); ok && classifyCode(kerr) == kindPermanent {
				return survey{}, &topicRefusedError{topic: name, err: info.Err}
			}
			s.pending = append(s.pending, name)
		}
	}
	slices.Sort(s.missing)
	slices.Sort(s.pending)
	return s, nil
}

// EnsureTopics checks that the deployment's four topics exist and are usable,
// and creates the missing ones when Config.CreateTopics is set. It is meant to
// run once at startup, so a missing topic surfaces there instead of as a stalled
// consumer or a failed produce. Nothing in the transport calls it.
//
// Checking never creates a topic: it only reads cluster metadata. Creation uses
// Config.Partitions and Config.ReplicationFactor and touches only topics that
// were missing, so a topic that exists is never altered or deleted. If its
// partition count differs from Config.Partitions, that is logged as a warning.
// A topic that another runner creates at the same moment counts as created.
//
// After creating topics it waits, with a bounded poll, for the cluster to
// report them usable. The wait ends with ctx, or after 30 seconds if ctx has no
// deadline.
func (t *Transport) EnsureTopics(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ensureWindow)
		defer cancel()
	}
	defer t.admin.Close()

	names := t.names.All()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kafka: ensure topics: %w", err)
	}
	s, err := t.survey(ctx, names)
	if err != nil {
		return err
	}
	existing := slices.DeleteFunc(slices.Clone(names), func(n string) bool { return slices.Contains(s.missing, n) })
	if len(s.missing) > 0 {
		if !t.cfg.CreateTopics {
			return fmt.Errorf("kafka: topics %s do not exist; create them in advance, or enable CreateTopics",
				strings.Join(s.missing, ", "))
		}
		if err := t.createMissing(ctx, s.missing); err != nil {
			return err
		}
	}
	// Created topics, and existing ones that are not usable yet, are awaited.
	if !s.ready() {
		if s, err = t.awaitReady(ctx, names); err != nil {
			return err
		}
	}
	t.warnPartitionDrift(existing, s.infos)
	return nil
}

// createMissing creates the topics and tolerates TopicAlreadyExists, which is
// what the loser of a race between two starting runners sees.
func (t *Transport) createMissing(ctx context.Context, missing []string) error {
	topics := make([]kafkago.TopicConfig, len(missing))
	for i, name := range missing {
		topics[i] = kafkago.TopicConfig{
			Topic:             name,
			NumPartitions:     t.cfg.Partitions,
			ReplicationFactor: t.cfg.ReplicationFactor,
		}
	}
	results, err := t.admin.CreateTopics(ctx, topics)
	if err != nil {
		return fmt.Errorf("kafka: create topics %s: %w", strings.Join(missing, ", "), err)
	}
	var errs []error
	for _, name := range missing {
		if err := results[name]; err != nil && !errors.Is(err, kafkago.TopicAlreadyExists) {
			errs = append(errs, fmt.Errorf("kafka: create topic %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// awaitReady polls until every topic is usable. Topic metadata spreads through
// the cluster after a create, so a freshly created topic can read as missing or
// without a leader for a short while; both are retried until ctx ends.
func (t *Transport) awaitReady(ctx context.Context, names []string) (survey, error) {
	wait := ensurePollMin
	var lastErr error
	for {
		s, err := t.survey(ctx, names)
		if refused, ok := errors.AsType[*topicRefusedError](err); ok {
			return survey{}, refused
		}
		if err == nil && s.ready() {
			return s, nil
		}
		detail := s.reasons()
		if err != nil {
			lastErr, detail = err, err.Error()
		}
		if !t.pause(ctx, wait) {
			cause := ctx.Err()
			if cause == nil {
				cause = lastErr
			}
			return survey{}, fmt.Errorf("kafka: topics not ready (%s): %w", detail, cause)
		}
		wait = min(wait*2, ensurePollMax)
	}
}

// warnPartitionDrift logs the topics that already existed with a partition
// count other than the configured one. It changes nothing: repartitioning a
// live topic reshuffles keys and is the operator's call.
func (t *Transport) warnPartitionDrift(existing []string, infos map[string]topicInfo) {
	if t.cfg.Partitions <= 0 {
		return
	}
	for _, name := range existing {
		if got := infos[name].Partitions; got != t.cfg.Partitions {
			t.warn("kafka topic has a different partition count than configured; leaving it unchanged",
				"topic", name, "partitions", got, "configured", t.cfg.Partitions)
		}
	}
}
