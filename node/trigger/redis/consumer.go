package redis

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

const (
	// consumerRetryDelay is the pause after a non-fatal read error. The kafka
	// trigger uses the same value for the same reason; the two are independent
	// constants that happen to coincide, not a contract.
	consumerRetryDelay = 100 * time.Millisecond

	// readBlock is how long one XREADGROUP parks inside Redis waiting for new
	// entries. It bounds two things at once: how quickly Close() is noticed
	// (the loop can only check ctx between reads) and how often an idle
	// consumer wakes up. It stays below go-redis's 3s default ReadTimeout so a
	// quiet stream never looks like a dead connection.
	readBlock = time.Second
)

// messageAcker is the optional half of Consumer, mirroring the kafka trigger's
// messageCommitter: acknowledgement is discovered by type assertion rather than
// being part of Consumer, because pub/sub has nothing to acknowledge and the
// scripted consumers in the tests should not have to implement it.
type messageAcker interface {
	Ack(ctx context.Context, msg Message) error
}

// redisConsumer serves both modes. They share everything except the loop body:
// the same close protocol, the same ownership rules for the message channel,
// and the same "the factory returns something already running" contract.
type redisConsumer struct {
	client *goredis.Client
	cfg    ConsumerConfig

	// ctx is created here rather than accepted from the caller. Cancelling it
	// is Close()'s job alone, so there is no second path that can stop the read
	// loop behind Close()'s back and leave done unclosed.
	ctx    context.Context
	cancel context.CancelFunc

	// pubsub is nil in stream mode. In pub/sub mode it must be closed to
	// unblock the loop: cancelling ctx does not reach the channel go-redis
	// hands out, so ctx alone would leave the loop parked forever.
	pubsub *goredis.PubSub

	messages chan Message
	done     chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newRedisConsumer(cfg ConsumerConfig) (Consumer, error) {
	if cfg.Addr == "" {
		return nil, errors.New("redis trigger requires an address")
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:        cfg.Addr,
		DB:          cfg.DB,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: cfg.DialTimeout,
	})

	// Deliberately no ping here. A trigger that refuses to activate because
	// Redis happened to be restarting would need something to re-activate it
	// later, and nothing does; the read loop retrying forever is the behaviour
	// that survives a blip. The kafka trigger does not connect eagerly either.
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &redisConsumer{
		client:   client,
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
		messages: make(chan Message, cfg.MaxInflight),
		done:     make(chan struct{}),
	}
	if cfg.Mode == "pubsub" {
		consumer.pubsub = client.Subscribe(ctx, cfg.Channel)
		go consumer.runPubSub()
		return consumer, nil
	}
	go consumer.runStream()
	return consumer, nil
}

func (c *redisConsumer) Messages() <-chan Message { return c.messages }

// Close is idempotent and blocks until the read loop has returned, so that when
// it returns the client really is shut down rather than merely asked to be.
func (c *redisConsumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		if c.pubsub != nil {
			c.closeErr = c.pubsub.Close()
		}
		<-c.done
	})
	return c.closeErr
}

// Ack is the stream half of at-least-once: the entry stays in the group's
// pending list until this runs, so a runner that dies mid-workflow leaves the
// entry to be reclaimed rather than losing it. Pub/sub has no equivalent —
// Redis has already forgotten the message by the time it arrives — so this
// reports success there rather than inventing a failure the caller cannot act
// on.
func (c *redisConsumer) Ack(ctx context.Context, msg Message) error {
	if c.cfg.Mode != "stream" {
		return nil
	}
	return c.client.XAck(ctx, msg.Stream, c.cfg.Group, msg.ID).Err()
}

// runStream owns c.messages: it is the only goroutine that writes to it and the
// only one that closes it, which is what makes a send-on-closed-channel panic
// structurally impossible rather than merely unlikely.
func (c *redisConsumer) runStream() {
	defer close(c.done)
	defer close(c.messages)
	defer func() { _ = c.client.Close() }()

	groupReady := false
	// The zero time makes the first cycle reclaim before its first read, so a
	// runner that restarts picks up whatever its previous life left pending
	// instead of waiting out a full interval first.
	var lastClaim time.Time

	for {
		if c.ctx.Err() != nil {
			return
		}
		if !groupReady {
			err := c.client.XGroupCreateMkStream(c.ctx, c.cfg.Stream, c.cfg.Group, c.cfg.StartID).Err()
			if err != nil && !isBusyGroupError(err) {
				if !sleepConsumerRetry(c.ctx) {
					return
				}
				continue
			}
			groupReady = true
		}

		if time.Since(lastClaim) >= c.cfg.ClaimMinIdle {
			if reclaimed, ok := c.reclaimPending(); !ok {
				return
			} else if reclaimed {
				lastClaim = time.Now()
			}
			// On failure lastClaim is left alone on purpose, so the next cycle
			// tries again rather than treating a failed reclaim as a done one.
			// The XREADGROUP below still runs, so a broken reclaim degrades
			// orphan recovery without also stopping new messages.
		}

		streams, err := c.client.XReadGroup(c.ctx, &goredis.XReadGroupArgs{
			Group:    c.cfg.Group,
			Consumer: c.cfg.Consumer,
			Streams:  []string{c.cfg.Stream, ">"},
			Count:    int64(c.cfg.MaxInflight),
			Block:    readBlock,
		}).Result()
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			// redis.Nil is how a BLOCK timeout reports "nothing arrived", which
			// is the normal state of a quiet stream, not an error to back off
			// from.
			if errors.Is(err, goredis.Nil) {
				continue
			}
			if !sleepConsumerRetry(c.ctx) {
				return
			}
			continue
		}
		for _, stream := range streams {
			if !c.deliver(stream.Stream, stream.Messages) {
				return
			}
		}
	}
}

// reclaimPending takes over entries that some consumer was handed and never
// acknowledged. Without it the group's pending list only grows: XREADGROUP with
// ">" returns entries nobody has seen, so a message whose runner died would sit
// there forever and "at-least-once" would be untrue.
//
// It returns whether the reclaim ran, and whether the loop should keep going.
func (c *redisConsumer) reclaimPending() (reclaimed bool, keepGoing bool) {
	entries, _, err := c.client.XAutoClaim(c.ctx, &goredis.XAutoClaimArgs{
		Stream:   c.cfg.Stream,
		Group:    c.cfg.Group,
		Consumer: c.cfg.Consumer,
		MinIdle:  c.cfg.ClaimMinIdle,
		Start:    "0-0",
		Count:    int64(c.cfg.MaxInflight),
	}).Result()
	if err != nil {
		if c.ctx.Err() != nil {
			return false, false
		}
		return false, true
	}
	return true, c.deliver(c.cfg.Stream, entries)
}

func (c *redisConsumer) deliver(stream string, entries []goredis.XMessage) bool {
	for _, entry := range entries {
		select {
		case c.messages <- streamMessage(stream, entry, c.cfg.PayloadField):
		case <-c.ctx.Done():
			return false
		}
	}
	return true
}

func (c *redisConsumer) runPubSub() {
	defer close(c.done)
	defer close(c.messages)
	defer func() { _ = c.client.Close() }()

	incoming := c.pubsub.Channel()
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-incoming:
			if !ok {
				return
			}
			select {
			case c.messages <- Message{
				Channel: msg.Channel,
				Payload: []byte(msg.Payload),
				Time:    time.Now(),
			}:
			case <-c.ctx.Done():
				return
			}
		}
	}
}

func streamMessage(stream string, entry goredis.XMessage, payloadField string) Message {
	// Copied rather than aliased: go-redis owns entry.Values, and the emitted
	// event outlives this call.
	values := make(map[string]any, len(entry.Values))
	maps.Copy(values, entry.Values)
	return Message{
		ID:      entry.ID,
		Stream:  stream,
		Values:  values,
		Payload: streamPayload(values, payloadField),
		Time:    streamEntryTime(entry.ID),
	}
}

// streamPayload picks what lands in TriggerEvent.Raw. With no payload_field
// configured the whole entry is encoded, which is lossless; with one
// configured, an entry that lacks that field yields an empty payload rather
// than being dropped, because Values still carries the entry verbatim and a
// silently discarded message would be worse than a visibly empty one.
func streamPayload(values map[string]any, payloadField string) []byte {
	if payloadField != "" {
		raw, ok := values[payloadField]
		if !ok {
			return nil
		}
		return []byte(cast.ToString(raw))
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil
	}
	return encoded
}

// streamEntryTime recovers the server-side timestamp Redis already put in the
// entry ID ("<unix-millis>-<sequence>"), so events carry when Redis accepted
// them rather than when this process happened to read them.
func streamEntryTime(id string) time.Time {
	millis, _, found := strings.Cut(id, "-")
	if !found {
		return time.Time{}
	}
	parsed, err := strconv.ParseInt(millis, 10, 64)
	if err != nil || parsed <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(parsed).UTC()
}

// isBusyGroupError distinguishes "the group already exists", which is the
// normal state on every activation after the first, from a real failure. Redis
// reports it as an error string with no distinct type, so matching the prefix
// is the only option go-redis leaves.
func isBusyGroupError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP")
}

func sleepConsumerRetry(ctx context.Context) bool {
	timer := time.NewTimer(consumerRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
