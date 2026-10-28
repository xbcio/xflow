package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// lagStatsInterval is the sampling interval of the lag integration tests: fast
// enough that a wait of seconds sees many rounds, slow enough not to flood the
// broker with offset requests.
const lagStatsInterval = 200 * time.Millisecond

// startLagConsumer is startConsumer with a sampling interval, so the lag
// sampler reports within a test's lifetime instead of every 30 seconds.
func startLagConsumer(t *testing.T, tr *Transport, h queue.TaskHandler) {
	t.Helper()
	rawStop, err := tr.StartConsumer(queue.ConsumerConfig{Concurrency: 4, StatsInterval: lagStatsInterval}, h)
	if err != nil {
		t.Fatalf("StartConsumer: %v", err)
	}
	var once sync.Once
	t.Cleanup(func() { once.Do(rawStop) })
}

// lastDLQ is the most recent dead-letter depth the observer saw, and whether it
// saw one at all.
func (o *lagObserver) lastDLQ() (int64, bool) {
	_, dlq := o.reports()
	if len(dlq) == 0 {
		return 0, false
	}
	return dlq[len(dlq)-1], true
}

// While the handlers hold their tasks nothing is committed, so the group's lag
// is the whole backlog; once they finish and the offsets are committed it falls
// to zero. The delay and batch groups have nothing to read and report zero.
func TestKafkaIntegrationLagCountsBacklogAndDrains(t *testing.T) {
	const total = 5
	e := newITEnv(t)
	obs := &lagObserver{}
	tr := e.transport(nil, WithObserver(obs))
	e.ensure(tr)
	for i := range total {
		if err := tr.Enqueue(t.Context(), itTask(execID("lag", i), false)); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	handler := func(ctx context.Context, _ *engine.Task) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	startLagConsumer(t, tr, handler)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	waitFor(t, "the default group's lag to show the whole backlog", 30*time.Second, func() bool {
		return obs.byRole()[groupDefault] == total
	})
	waitFor(t, "a lag report for every consumed topic", 30*time.Second, func() bool {
		return len(obs.byRole()) == len(LagRoles())
	})
	stays(t, 1500*time.Millisecond, func() error {
		if got := obs.byRole(); got[groupDefault] != total || got[groupBatch] != 0 || got[groupDelay] != 0 {
			t.Logf("lag by role: %v", got)
			return errLagMoved
		}
		return nil
	})

	releaseOnce.Do(func() { close(release) })
	waitFor(t, "the lag to drain once the backlog is handled", 30*time.Second, func() bool {
		return obs.byRole()[groupDefault] == 0 && e.committed(groupDefault, e.names.Default(), 3) == total
	})
}

type lagError string

func (e lagError) Error() string { return string(e) }

const errLagMoved = lagError("lag moved while the handlers held every task")

// A task that fails permanently ends up on the dead-letter topic, and the depth
// goes from the zero reported for the empty topic to one.
func TestKafkaIntegrationDLQDepthCountsDeadLetters(t *testing.T) {
	e := newITEnv(t)
	obs := &lagObserver{}
	tr := e.transport(nil, WithObserver(obs))
	e.ensure(tr)
	c := &collector{fail: func(delivery, int) error { return &permanentErr{} }}
	startLagConsumer(t, tr, c.handle)

	waitFor(t, "a dead-letter depth of zero for the empty topic", 30*time.Second, func() bool {
		d, ok := obs.lastDLQ()
		return ok && d == 0
	})
	if err := tr.Enqueue(t.Context(), itTask("doomed", false)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, "a dead-letter depth of one", 30*time.Second, func() bool {
		d, _ := obs.lastDLQ()
		return d == 1
	})
	stays(t, 1500*time.Millisecond, func() error {
		if d, _ := obs.lastDLQ(); d != 1 {
			t.Logf("dead-letter depth: %d", d)
			return errLagMoved
		}
		return nil
	})
}

// Sampling topics that were never created reports nothing, logs why, and does
// not make the broker create them: a topic that is missing is not an empty one.
func TestKafkaIntegrationLagOfMissingTopicsIsNotReported(t *testing.T) {
	e := newITEnv(t)
	obs := &lagObserver{}
	log := &recordingLogger{}
	tr := e.transport(nil, WithObserver(obs), WithLogger(log))
	s := &lagSampler{client: tr.offsets, names: tr.names, obs: obs, warn: tr.warn}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	s.sample(ctx)

	if lags, dlq := obs.reports(); len(lags) != 0 || len(dlq) != 0 {
		t.Errorf("reports for topics that do not exist: lags %v, dlq %v", lags, dlq)
	}
	if len(log.warned()) == 0 {
		t.Error("sampling missing topics logged nothing")
	}
	for _, topic := range e.names.All() {
		if e.topicExists(topic) {
			t.Errorf("sampling made the broker create %s", topic)
		}
	}
}
