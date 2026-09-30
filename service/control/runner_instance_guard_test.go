package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// guardDirectories runs one test body against both RunnerDirectory
// implementations, so the memory guard and its Redis Lua mirror cannot drift.
func guardDirectories() map[string]func(t *testing.T) RunnerDirectory {
	return map[string]func(t *testing.T) RunnerDirectory{
		"memory": func(t *testing.T) RunnerDirectory { return NewMemoryRunnerDirectory() },
		"redis": func(t *testing.T) RunnerDirectory {
			_, rdb := newRedisRunnerDirectoryTestClient(t)
			return NewRedisRunnerDirectory(rdb)
		},
	}
}

func guardRegisterRequest(uid string, now time.Time) RegisterRunnerRequest {
	return RegisterRunnerRequest{
		RunnerID:     "runner-shared",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		InstanceUID:  uid,
		Now:          now,
	}
}

// TestRunnerDirectoryInstanceGuard: a registration from a different instance
// must not evict a live session, while the same instance, a stale session,
// and legacy (UID-less) registrations keep the old takeover.
func TestRunnerDirectoryInstanceGuard(t *testing.T) {
	cases := []struct {
		name       string
		currentUID string
		newUID     string
		elapsed    time.Duration
		wantErr    error
	}{
		{name: "different live instance is refused", currentUID: "pod:a", newUID: "pod:b", elapsed: time.Second, wantErr: ErrRunnerIDConflict},
		{name: "same instance reclaims its session", currentUID: "pod:a", newUID: "pod:a", elapsed: time.Second},
		{name: "different instance after the live window takes over", currentUID: "pod:a", newUID: "pod:b", elapsed: DefaultRunnerLiveTTL + time.Second},
		{name: "legacy registration without uid keeps takeover", currentUID: "pod:a", newUID: "", elapsed: time.Second},
		{name: "legacy session without uid can be taken over", currentUID: "", newUID: "pod:b", elapsed: time.Second},
	}
	for dirName, newDir := range guardDirectories() {
		for _, tc := range cases {
			t.Run(dirName+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				dir := newDir(t)
				base := time.Now().UTC().Truncate(time.Millisecond)
				first, err := dir.Register(ctx, guardRegisterRequest(tc.currentUID, base))
				if err != nil {
					t.Fatalf("first register: %v", err)
				}
				second, err := dir.Register(ctx, guardRegisterRequest(tc.newUID, base.Add(tc.elapsed)))
				if tc.wantErr != nil {
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("second register = %v, want %v", err, tc.wantErr)
					}
					if err := dir.ValidateSession(ctx, "runner-shared", first.SessionID); err != nil {
						t.Fatalf("live session after refused register: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("second register: %v", err)
				}
				if err := dir.ValidateSession(ctx, "runner-shared", second.SessionID); err != nil {
					t.Fatalf("replacement session: %v", err)
				}
				if err := dir.ValidateSession(ctx, "runner-shared", first.SessionID); !errors.Is(err, ErrRunnerSessionStale) {
					t.Fatalf("old session = %v, want ErrRunnerSessionStale", err)
				}
			})
		}
	}
}

// Liveness is the last heartbeat, not the registration time.
func TestRunnerDirectoryInstanceGuardFollowsHeartbeat(t *testing.T) {
	for name, newDir := range guardDirectories() {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := newDir(t)
			base := time.Now().UTC().Truncate(time.Millisecond)
			first, err := dir.Register(ctx, guardRegisterRequest("pod:a", base))
			if err != nil {
				t.Fatalf("register: %v", err)
			}
			beat := base.Add(DefaultRunnerLiveTTL)
			if err := dir.Heartbeat(ctx, HeartbeatRequest{RunnerID: "runner-shared", SessionID: first.SessionID, Capacity: 1, Now: beat}); err != nil {
				t.Fatalf("heartbeat: %v", err)
			}
			if _, err := dir.Register(ctx, guardRegisterRequest("pod:b", beat.Add(time.Second))); !errors.Is(err, ErrRunnerIDConflict) {
				t.Fatalf("register after heartbeat = %v, want ErrRunnerIDConflict", err)
			}
		})
	}
}

// Deregister ends the live window of the current session only.
func TestRunnerDirectoryDeregisterEndsLiveWindow(t *testing.T) {
	for name, newDir := range guardDirectories() {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := newDir(t)
			d, ok := dir.(RunnerDeregisterer)
			if !ok {
				t.Fatalf("%T does not implement RunnerDeregisterer", dir)
			}
			base := time.Now().UTC().Truncate(time.Millisecond)
			first, err := dir.Register(ctx, guardRegisterRequest("pod:a", base))
			if err != nil {
				t.Fatalf("register: %v", err)
			}
			if err := d.Deregister(ctx, "runner-shared", "not-the-session"); !errors.Is(err, ErrRunnerSessionStale) {
				t.Fatalf("wrong session = %v, want ErrRunnerSessionStale", err)
			}
			if err := d.Deregister(ctx, "runner-unknown", first.SessionID); !errors.Is(err, ErrRunnerNotFound) {
				t.Fatalf("unknown runner = %v, want ErrRunnerNotFound", err)
			}
			if err := d.Deregister(ctx, "runner-shared", first.SessionID); err != nil {
				t.Fatalf("deregister: %v", err)
			}
			second, err := dir.Register(ctx, guardRegisterRequest("pod:b", base.Add(time.Second)))
			if err != nil {
				t.Fatalf("register after deregister = %v, want immediate takeover", err)
			}
			if err := d.Deregister(ctx, "runner-shared", first.SessionID); !errors.Is(err, ErrRunnerSessionStale) {
				t.Fatalf("late deregister = %v, want ErrRunnerSessionStale", err)
			}
			if _, err := dir.Register(ctx, guardRegisterRequest("pod:c", base.Add(2*time.Second))); !errors.Is(err, ErrRunnerIDConflict) {
				t.Fatalf("register against replacement = %v, want ErrRunnerIDConflict", err)
			}
			if err := dir.ValidateSession(ctx, "runner-shared", second.SessionID); err != nil {
				t.Fatalf("replacement session: %v", err)
			}
		})
	}
}

// TestHandleDeregisterHTTP covers the endpoint end to end: a wrong token is
// refused before the directory is touched, the right one returns 204 and
// unprotects the session, and a conflict renders as 409.
func TestHandleDeregisterHTTP(t *testing.T) {
	core, _, token := renewIdentityFixture(t, 0, "runner-a")
	dir := NewMemoryRunnerDirectory()
	core.runners = dir
	register := func(uid string) error {
		_, err := dir.Register(context.Background(), RegisterRunnerRequest{RunnerID: "runner-a", Capacity: 1, InstanceUID: uid, Now: time.Now()})
		return err
	}
	if err := register("pod:a"); err != nil {
		t.Fatalf("register: %v", err)
	}
	snapshot, ok := dir.Runner(context.Background(), "runner-a")
	if !ok {
		t.Fatal("runner-a not registered")
	}
	ts := httptest.NewServer((&Server{core: core}).Handler())
	defer ts.Close()

	post := func(bearer string) int {
		t.Helper()
		body := strings.NewReader(`{"runner_id":"runner-a","session_id":"` + snapshot.SessionID + `"}`)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+protocol.DeregisterPath, body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("wrong-token"); code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", code)
	}
	if err := register("pod:b"); !errors.Is(err, ErrRunnerIDConflict) {
		t.Fatalf("session must stay protected after a refused deregister, got %v", err)
	}
	if code := post(token); code != http.StatusNoContent {
		t.Fatalf("deregister status = %d, want 204", code)
	}
	if err := register("pod:b"); err != nil {
		t.Fatalf("register after deregister: %v", err)
	}
	rec := httptest.NewRecorder()
	writeRunnerError(rec, ErrRunnerIDConflict)
	if rec.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", rec.Code)
	}
}
