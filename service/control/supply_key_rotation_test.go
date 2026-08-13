package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/service/protocol"
)

// Rotation is only useful if every replica ends up encrypting with the rotated
// key. A Rotate() that mutates process-local state and never touches the Redis
// rendezvous key leaves replica B encrypting with the pre-rotation key while
// replica A hands runners the new one -- and a restart of A silently reverts to
// the pre-rotation key still sitting in Redis.
func TestRotatePropagatesToOtherReplicas(t *testing.T) {
	rdb := newMiniRedis(t)
	ctx := context.Background()
	const key = "xflow:supply:transport-key-test"

	a, err := NewSupplyEncryptorShared(ctx, rdb, key)
	if err != nil {
		t.Fatalf("replica A: %v", err)
	}
	b, err := NewSupplyEncryptorShared(ctx, rdb, key)
	if err != nil {
		t.Fatalf("replica B: %v", err)
	}

	before := a.KeyForRunner()
	if err := a.Rotate(ctx); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	rotated := a.KeyForRunner()
	if rotated == before {
		t.Fatal("Rotate did not change the key on the rotating replica")
	}

	// B only notices through Refresh -- that is the whole point of the shared
	// rendezvous key, and what the production refresh tick calls.
	if _, err := b.Refresh(ctx); err != nil {
		t.Fatalf("replica B refresh: %v", err)
	}
	if got := b.KeyForRunner(); got != rotated {
		t.Fatal("replica B still holds the pre-rotation key: a runner that " +
			"received the rotated key from A cannot decrypt content encrypted by B")
	}

	// A fresh replica starting after the rotation must adopt the rotated key,
	// not the stale value left in Redis.
	c, err := NewSupplyEncryptorShared(ctx, rdb, key)
	if err != nil {
		t.Fatalf("replica C: %v", err)
	}
	if got := c.KeyForRunner(); got != rotated {
		t.Fatal("a replica starting after the rotation adopted the stale key from Redis")
	}
}

// The rotation must reach every registered runner, not just whichever one
// heartbeats first. A runner left on the old key fails to decrypt supply
// content, its gate declines, and it hosts no triggers -- while heartbeating
// perfectly healthily.
func TestRotationReachesEveryRunner(t *testing.T) {
	srv := NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory())
	enc, err := NewSupplyEncryptor()
	if err != nil {
		t.Fatalf("NewSupplyEncryptor: %v", err)
	}
	srv.core.supplyEncryptor = enc
	ctx := context.Background()

	// Each runner reports the key it holds. At registration that is the
	// pre-rotation key.
	held := keyIDOf(t, enc.KeyForRunner())

	sessions := map[string]string{}
	for _, id := range []string{"runner-a", "runner-b"} {
		reg, regErr := srv.core.register(ctx, protocol.RegisterRunnerRequest{
			RunnerID: id, Concurrency: 1, SupportsEncryption: true,
		}, TransportInfo{})
		if regErr != nil {
			t.Fatalf("register %s: %v", id, regErr)
		}
		sessions[id] = reg.SessionID
	}

	if err := enc.Rotate(ctx); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	adopted := map[string]string{}
	for id, session := range sessions {
		resp, hbErr := srv.core.heartbeat(ctx, protocol.HeartbeatRequest{
			RunnerID: id, SessionID: session, Capacity: 1, SupplyKeyID: held,
		}, TransportInfo{})
		if hbErr != nil {
			t.Fatalf("heartbeat %s: %v", id, hbErr)
		}
		if resp.SupplyKeyRotation == "" {
			t.Fatalf("%s never received the rotation; it is stuck on the old key "+
				"and will fail every supply decrypt", id)
		}
		adopted[id] = keyIDOf(t, resp.SupplyKeyRotation)
	}

	// A second heartbeat must NOT redeliver: installSupplyKey calls
	// Keyring.Rotate on each delivery, so redelivering demotes the key it just
	// promoted and evicts the previous one.
	for id, session := range sessions {
		resp, hbErr := srv.core.heartbeat(ctx, protocol.HeartbeatRequest{
			RunnerID: id, SessionID: session, Capacity: 1, SupplyKeyID: adopted[id],
		}, TransportInfo{})
		if hbErr != nil {
			t.Fatalf("second heartbeat %s: %v", id, hbErr)
		}
		if resp.SupplyKeyRotation != "" {
			t.Fatalf("%s got the rotation redelivered; its keyring loses the previous key", id)
		}
	}
}

// A runner that was down during the rotation, or joined afterwards, still
// reports the key it holds -- so it converges on its very next heartbeat with
// no per-runner bookkeeping on the server.
func TestRotationReachesARunnerThatMissedIt(t *testing.T) {
	srv := NewServer(&fakeControlEngine{}, NewMemoryRunnerDirectory())
	enc, err := NewSupplyEncryptor()
	if err != nil {
		t.Fatalf("NewSupplyEncryptor: %v", err)
	}
	srv.core.supplyEncryptor = enc
	ctx := context.Background()

	stale := keyIDOf(t, enc.KeyForRunner())
	if err := enc.Rotate(ctx); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Registers only after the rotation has already happened.
	reg, err := srv.core.register(ctx, protocol.RegisterRunnerRequest{
		RunnerID: "latecomer", Concurrency: 1, SupportsEncryption: true,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp, err := srv.core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: "latecomer", SessionID: reg.SessionID, Capacity: 1, SupplyKeyID: stale,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if resp.SupplyKeyRotation == "" {
		t.Fatal("a runner holding a superseded key was not given the current one; " +
			"rotation delivery is not convergent")
	}
}

func keyIDOf(t *testing.T, encoded string) string {
	t.Helper()
	k, err := supplyenc.KeyFromBase64(encoded)
	if err != nil {
		t.Fatalf("decode key: %v", err)
	}
	return k.ID
}

func TestClampSupplyKeyRotationPeriod(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"zero adopts the default", 0, DefaultSupplyKeyRotationPeriod},
		{"negative disables rotation", -time.Second, 0},
		{"below the floor is raised", time.Second, minSupplyKeyRotationPeriod},
		{"positive is kept", 2 * time.Hour, 2 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampSupplyKeyRotationPeriod(tc.in); got != tc.want {
				t.Errorf("clampSupplyKeyRotationPeriod(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// Only one replica per period may rotate; otherwise a fleet of N replicas
// rotates N times per period and runners spend their time chasing keys.
func TestRotationSlotIsClaimedOncePerPeriod(t *testing.T) {
	rdb := newMiniRedis(t)
	ctx := context.Background()
	const key = "xflow:supply:transport-key-slot-test"

	a, err := NewSupplyEncryptorShared(ctx, rdb, key)
	if err != nil {
		t.Fatalf("replica A: %v", err)
	}
	b, err := NewSupplyEncryptorShared(ctx, rdb, key)
	if err != nil {
		t.Fatalf("replica B: %v", err)
	}

	first, err := a.claimRotationSlot(ctx, time.Hour)
	if err != nil {
		t.Fatalf("replica A claim: %v", err)
	}
	if !first {
		t.Fatal("the first claim of a free slot was declined")
	}
	second, err := b.claimRotationSlot(ctx, time.Hour)
	if err != nil {
		t.Fatalf("replica B claim: %v", err)
	}
	if second {
		t.Fatal("a second replica claimed the same period's slot; the fleet " +
			"rotates once per replica instead of once per period")
	}
}
