package control

import (
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestDrainPushesByCredit(t *testing.T) {
	p := NewRunnerPool()
	p.RegisterWithLabelsAndPolicy("r1", 3,
		[]protocol.Capability{{NodeType: "xflow.function"}}, nil,
		RunnerPolicy{AllowedNodeTypes: []string{"*"}})
	sendCh := make(chan protocol.ServerFrame, 4)
	sess := newStreamSession("r1", sendCh, make(chan struct{}), 2)
	p.bindSession("r1", sess)

	ids := []engine.LeaseID{"L1", "L2", "L3"}
	for _, id := range ids {
		if err := p.Assign(engine.TaskLease{LeaseID: id, NodeType: "xflow.function"}); err != nil {
			t.Fatalf("Assign(%s): %v", id, err)
		}
	}

	pushed := p.drainInto(sess)
	if pushed != 2 {
		t.Fatalf("drain pushed %d, want 2", pushed)
	}
	if len(p.runners["r1"].queue) != 1 {
		t.Fatalf("queue len = %d, want 1", len(p.runners["r1"].queue))
	}

	// Receive the 2 frames and verify LeaseIDs
	got1 := <-sendCh
	got2 := <-sendCh
	if got1.Task == nil || got1.Task.Lease == nil {
		t.Fatal("first frame has no Task/Lease")
	}
	if got2.Task == nil || got2.Task.Lease == nil {
		t.Fatal("second frame has no Task/Lease")
	}
	if got1.Task.Lease.LeaseID != "L1" {
		t.Fatalf("first frame LeaseID = %q, want L1", got1.Task.Lease.LeaseID)
	}
	if got2.Task.Lease.LeaseID != "L2" {
		t.Fatalf("second frame LeaseID = %q, want L2", got2.Task.Lease.LeaseID)
	}
}

func TestConsumeResultReplenishesCredit(t *testing.T) {
	p := NewRunnerPool()
	p.RegisterWithLabelsAndPolicy("r1", 1,
		[]protocol.Capability{{NodeType: "xflow.function"}}, nil,
		RunnerPolicy{AllowedNodeTypes: []string{"*"}})
	sendCh := make(chan protocol.ServerFrame, 4)
	sess := newStreamSession("r1", sendCh, make(chan struct{}), 1)
	p.bindSession("r1", sess)

	if err := p.Assign(engine.TaskLease{LeaseID: "L1", NodeType: "xflow.function"}); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	p.drainInto(sess)
	if sess.credit() != 0 {
		t.Fatalf("credit after drain = %d, want 0", sess.credit())
	}
	p.consumeResult("r1", "L1")
	if sess.credit() != 1 {
		t.Fatalf("credit after result = %d, want 1", sess.credit())
	}
}
