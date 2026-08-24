package kafka

import (
	"net"
	"testing"
	"time"
)

// TestKafkaGoConsumerCloseInterruptsBlockedFetch exercises the actual
// kafka-go Reader, not the package's channel fakes. The local TCP peer accepts
// the reader's broker connection and deliberately never replies, leaving
// FetchMessage blocked inside kafka-go. Close must cancel that network wait and
// join the consumer goroutine within the trigger's one-second shutdown budget.
func TestKafkaGoConsumerCloseInterruptsBlockedFetch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		accepted <- conn
	}()

	tuning := defaultTuning()
	// Deliberately much longer than the Close assertion: a passing test proves
	// cancellation interrupted the blocked operation, not that Dial timed out.
	tuning.DialTimeout = 10 * time.Second
	consumerRaw, err := newKafkaGoConsumer(ConsumerConfig{
		Brokers:     []string{listener.Addr().String()},
		Topic:       "close-probe",
		Group:       "close-probe-group",
		StartOffset: "earliest",
		MaxInflight: 1,
		Tuning:      tuning,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer := consumerRaw.(*kafkaGoConsumer)

	var peer net.Conn
	select {
	case peer = <-accepted:
		defer func() { _ = peer.Close() }()
	case <-time.After(3 * time.Second):
		_ = consumer.Close()
		t.Fatal("kafka-go Reader never connected to the local broker stub; FetchMessage was not proven blocked")
	}

	started := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- consumer.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("Close took %s, want under 1s", elapsed)
		}
	case <-time.After(time.Second):
		// Unstick a broken implementation before failing so this test does not
		// leak its real Reader goroutine into the rest of the package suite.
		_ = peer.Close()
		_ = listener.Close()
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
		}
		t.Fatal("Close did not interrupt kafka-go FetchMessage within 1s")
	}
}
