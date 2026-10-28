package kafka

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// topicPartitions asks the broker, without creating anything, how many
// partitions topic has. It returns 0 when the topic does not exist.
func (e *itEnv) topicPartitions(topic string) int {
	e.t.Helper()
	tr := &kafkago.Transport{}
	defer tr.CloseIdleConnections()
	client := &kafkago.Client{Addr: kafkago.TCP(e.brokers...), Timeout: 10 * time.Second, Transport: tr}
	res, err := client.Metadata(e.t.Context(), &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		e.t.Fatalf("metadata %s: %v", topic, err)
	}
	for _, tp := range res.Topics {
		if tp.Name == topic && tp.Error == nil {
			return len(tp.Partitions)
		}
	}
	return 0
}

// With CreateTopics off, EnsureTopics only checks. The broker auto-creates
// topics on metadata requests that ask for it, so the test also proves the
// check did not ask: afterwards the four topics must still be absent.
func TestKafkaIntegrationEnsureTopicsWithoutCreateOnlyChecks(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(func(c *Config) { c.CreateTopics, c.Partitions, c.ReplicationFactor = false, 0, 0 })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err := tr.EnsureTopics(ctx)
	if err == nil {
		t.Fatal("EnsureTopics succeeded although no topic exists and CreateTopics is off")
	}
	t.Logf("EnsureTopics: %v", err)
	for _, name := range e.names.All() {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not name the missing topic %s: %v", name, err)
		}
	}
	// An auto-creation triggered by the check would show up within this window.
	stays(t, time.Second, func() error {
		for _, name := range e.names.All() {
			if n := e.topicPartitions(name); n != 0 {
				return fmt.Errorf("topic %s exists with %d partitions; the check made the broker create it", name, n)
			}
		}
		return nil
	})
}

// CreateTopics makes all four topics with the configured partition count, a
// second call changes nothing, and several runners starting at once on the same
// deployment all succeed: whoever loses the creation race sees
// TopicAlreadyExists and carries on.
func TestKafkaIntegrationEnsureTopicsCreatesAndIsIdempotent(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(nil)
	e.ensure(tr)
	for _, name := range e.names.All() {
		if n := e.topicPartitions(name); n != 3 {
			t.Errorf("topic %s has %d partitions, want 3", name, n)
		}
	}
	e.ensure(tr)

	// A second deployment, so the race is for topics that do not exist yet.
	race := newITEnv(t)
	const runners = 4
	trs := make([]*Transport, runners)
	for i := range trs {
		trs[i] = race.transport(nil)
	}
	errs := make([]error, runners)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, rt := range trs {
		wg.Go(func() {
			<-start
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			errs[i] = rt.EnsureTopics(ctx)
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("runner %d: EnsureTopics: %v", i, err)
		}
	}
	for _, name := range race.names.All() {
		if n := race.topicPartitions(name); n != 3 {
			t.Errorf("raced topic %s has %d partitions, want 3", name, n)
		}
	}
}

// A topic that already exists with another partition count is left alone and
// reported with a warning, never an error.
func TestKafkaIntegrationEnsureTopicsWarnsOnPartitionDrift(t *testing.T) {
	e := newITEnv(t)
	e.ensure(e.transport(func(c *Config) { c.Partitions = 1 }))

	log := &recordingLogger{}
	tr := e.transport(nil, WithLogger(log))
	e.ensure(tr)

	for _, name := range e.names.All() {
		if n := e.topicPartitions(name); n != 1 {
			t.Errorf("topic %s has %d partitions, want it left at 1", name, n)
		}
	}
	var drifted []string
	for _, w := range log.warned() {
		if !strings.Contains(w.msg, "different partition count") {
			t.Errorf("unexpected warning %q %v", w.msg, w.args)
			continue
		}
		if i := slices.Index(w.args, any("topic")); i >= 0 && i+1 < len(w.args) {
			drifted = append(drifted, w.args[i+1].(string))
		}
	}
	slices.Sort(drifted)
	want := slices.Sorted(slices.Values(e.names.All()))
	if !slices.Equal(drifted, want) {
		t.Errorf("drift warnings for %v, want %v", drifted, want)
	}
}
