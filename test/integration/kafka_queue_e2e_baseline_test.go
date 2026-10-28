//go:build integration

package integration

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestKafkaQueueE2E_AsynqBaseline runs the basic workflow on the default Asynq
// transport (same Redis, its own deployment suffix, no Kafka node at all) and on
// Kafka, and requires the terminal state, every node's status and the output of
// the branch that ran to be identical. This is what shows that the transport is
// the only thing that differs.
func TestKafkaQueueE2E_AsynqBaseline(t *testing.T) {
	for _, n := range []int{7, 2} {
		asynqEnv := newKafkaE2EEnv(t)
		asynqNode := startKafkaE2ENode(t, asynqEnv, kafkaE2EOpts{asynq: true})
		if asynqEnv.kafkaUsed {
			t.Fatal("the baseline env must not touch Kafka")
		}
		kafkaEnv := newKafkaE2EEnv(t)
		kafkaNode := startKafkaE2ENode(t, kafkaEnv, kafkaE2EOpts{})

		want := runE2EBasic(t, asynqEnv, asynqNode, n)
		got := runE2EBasic(t, kafkaEnv, kafkaNode, n)
		if want.Status != types.ExecutionStatusSuccess {
			t.Fatalf("n=%d: asynq baseline ended %s (nodes %v), want success", n, want.Status, want.Nodes)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("n=%d: kafka run differs from the asynq baseline\n  kafka: %+v\n  asynq: %+v", n, got, want)
		}
		t.Logf("n=%d: kafka and asynq agree: %s %v", n, got.Status, got.Output)
	}
}
