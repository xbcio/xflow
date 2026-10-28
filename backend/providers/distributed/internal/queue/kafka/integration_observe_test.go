package kafka

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// logLine is one line a kafka-go reader printed, with the time since the test
// started its consumer.
type logLine struct {
	at   time.Duration
	text string
}

// readerLog collects what the kafka-go readers of one transport print. It keeps
// the lines instead of logging them: a reader goroutine may print after the
// test function has returned, and t.Logf must not be called then.
type readerLog struct {
	start time.Time
	mu    sync.Mutex
	lines []logLine
}

func (l *readerLog) print(format string, args ...any) {
	text := strings.TrimSpace(fmt.Sprintf(format, args...))
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, logLine{at: time.Since(l.start), text: text})
}

// times returns when lines containing every one of subs were printed.
func (l *readerLog) times(subs ...string) []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []time.Duration
	for _, line := range l.lines {
		match := true
		for _, s := range subs {
			match = match && strings.Contains(line.text, s)
		}
		if match {
			out = append(out, line.at.Round(10*time.Millisecond))
		}
	}
	return out
}

func (l *readerLog) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

// loggingReaders is a reader factory that builds the real group readers, with
// the logs of kafka-go turned on, which readerConfig leaves off.
func loggingReaders(e *itEnv, l *readerLog) readerFactory {
	return func(topic, groupID string) messageReader {
		rc, err := readerConfig(e.itConfig(), topic, groupID)
		if err != nil {
			e.t.Fatalf("readerConfig: %v", err)
		}
		rc.Logger = kafkago.LoggerFunc(l.print)
		rc.ErrorLogger = kafkago.LoggerFunc(l.print)
		return kafkago.NewReader(rc)
	}
}

// Exploration, not a contract: what a consumer does when the topics were never
// created, on a broker that auto-creates topics. The test records when the
// topics appear, how often each group rejoins, and when each group is first
// given a partition; then it produces one task per topic and waits probeWindow
// for delivery. It asserts only what is stable: the
// readers logged and nothing but the two probes was delivered. The rest is in
// the test log, because the outcome varies from run to run.
func TestKafkaIntegrationConsumerWithMissingTopicsObserve(t *testing.T) {
	const (
		window      = 15 * time.Second
		probeWindow = 60 * time.Second
	)
	if testing.Short() {
		t.Skip("observes the broker for over a minute")
	}
	e := newITEnv(t)
	rl := &readerLog{start: time.Now()}
	tr := e.transport(nil, withReaderFactory(loggingReaders(e, rl)))

	c := &collector{}
	stop := startConsumer(t, tr, c.handle)
	appeared := map[string]time.Duration{}
	for time.Since(rl.start) < window {
		for _, name := range e.names.All() {
			if _, seen := appeared[name]; !seen && e.topicPartitions(name) > 0 {
				appeared[name] = time.Since(rl.start).Round(10 * time.Millisecond)
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-t.Context().Done():
		}
	}
	partitions := map[string]int{}
	for _, name := range e.names.All() {
		partitions[name] = e.topicPartitions(name)
	}
	// By now the topics exist, so a task can be produced. Whether a group that
	// was given no partition when it joined ever picks it up is what this shows.
	probes := map[string]time.Duration{}
	sent := time.Now()
	for _, batch := range []bool{false, true} {
		id := "probe-default"
		if batch {
			id = "probe-batch"
		}
		if err := tr.Enqueue(t.Context(), itTask(execID(id, 0), batch)); err != nil {
			t.Logf("probe %s could not be enqueued: %v", id, err)
		}
	}
	probeEnd := time.Now().Add(probeWindow)
	for time.Now().Before(probeEnd) && len(probes) < 2 {
		for _, d := range c.all() {
			if _, seen := probes[string(d.id)]; !seen {
				probes[string(d.id)] = d.at.Sub(sent).Round(10 * time.Millisecond)
			}
		}
		select {
		case <-time.After(itStep):
		case <-t.Context().Done():
		}
	}
	stop()

	t.Logf("observed %v with no topics created: %d reader log lines", window, rl.total())
	for _, name := range e.names.All() {
		if at, ok := appeared[name]; ok {
			t.Logf("topic %s: appeared after %v with %d partitions (nobody called EnsureTopics)", name, at, partitions[name])
		} else {
			t.Logf("topic %s: never appeared", name)
		}
	}
	for _, group := range []string{groupDefault, groupBatch, groupDelay} {
		id := e.names.Group(group)
		joins := rl.times("joined group "+id, "in generation")
		t.Logf("group %s: joined %d times at %v", group, len(joins), joins)
	}
	// The error line does not name its group, so the lookups are counted together.
	t.Logf("partition lookups that failed with an unknown topic: %v", rl.times("Problem getting partitions"))
	for _, topic := range []string{e.names.Default(), e.names.Batch(), e.names.Delay()} {
		t.Logf("topic %s: first assigned to a member at %v", topic, firstOf(rl.times("subscribed to topics and partitions: map[{topic:"+topic)))
	}
	for id, after := range probes {
		t.Logf("probe %s: delivered %v after it was produced", id, after)
	}
	if len(probes) < 2 {
		t.Logf("probes delivered: %d of 2 within %v (a group that never rejoined cannot see its topic)", len(probes), probeWindow)
	}
	for _, d := range c.all() {
		if d.id != execID("probe-default", 0) && d.id != execID("probe-batch", 0) {
			t.Errorf("handler ran for %s, which nobody produced", d.id)
		}
	}
	if rl.total() == 0 {
		t.Error("the readers printed nothing; the logging factory was not used")
	}
}

// firstOf is the first of ts, or a note that there was none.
func firstOf(ts []time.Duration) string {
	if len(ts) == 0 {
		return "never"
	}
	return ts[0].String()
}
