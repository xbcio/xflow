package backend

import (
	"context"
	"testing"
	"time"
)

func TestAlwaysLeaderIsLeaderImmediately(t *testing.T) {
	var l LeaderElector = AlwaysLeader{}
	if !l.IsLeader() {
		t.Fatal("AlwaysLeader.IsLeader() = false, want true")
	}
}

func TestAlwaysLeaderCampaignReturnsImmediately(t *testing.T) {
	l := AlwaysLeader{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Campaign(ctx); err != nil {
		t.Fatalf("Campaign() error = %v, want nil", err)
	}
}

func TestAlwaysLeaderNotifyEmitsTrueOnce(t *testing.T) {
	l := AlwaysLeader{}
	select {
	case v := <-l.Notify():
		if !v {
			t.Fatal("Notify() first value = false, want true")
		}
	case <-time.After(time.Second):
		t.Fatal("Notify() did not emit within 1s")
	}
}

func TestAlwaysLeaderResignReturnsNil(t *testing.T) {
	l := AlwaysLeader{}
	if err := l.Resign(context.Background()); err != nil {
		t.Fatalf("Resign() error = %v, want nil", err)
	}
}
