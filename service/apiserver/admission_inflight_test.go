package apiserver

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/xbcio/xflow/observability/metrics"
)

// TestInflightGuardRejectsWhenFullAndReleases holds the bound full with
// handlers parked on a channel, so "full" is a fact of the test rather than a
// race against timing.
func TestInflightGuardRejectsWhenFullAndReleases(t *testing.T) {
	a := newAPIAdmission(APILimits{MaxInflight: 2}, nil, nil, nil)
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	h := a.guard(routeClassExecute, writeServerOverloaded, func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	})

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h(rec, httptest.NewRequest(http.MethodPost, "/", nil))
			if rec.Code != http.StatusOK {
				t.Errorf("parked request status = %d, want 200", rec.Code)
			}
		}()
	}
	<-entered
	<-entered

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("third request status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	env := decodeEnvelope(t, rec.Result(), nil)
	if env.Code != "server_overloaded" || env.Success {
		t.Fatalf("envelope = %+v, want server_overloaded failure", env)
	}

	close(release)
	wg.Wait()
	if got := a.inflight.inUse(); got != 0 {
		t.Fatalf("in use after release = %d, want 0", got)
	}
	// Slots freed by completed requests are reusable.
	rec = httptest.NewRecorder()
	a.guard(routeClassExecute, writeServerOverloaded, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("after release status = %d, want 200", rec.Code)
	}
}

// A handler that panics must still release its slot, or every panic would
// permanently shrink the replica's capacity.
func TestInflightGuardReleasesOnPanic(t *testing.T) {
	a := newAPIAdmission(APILimits{MaxInflight: 1}, nil, nil, nil)
	h := a.guard(routeClassExecution, writeServerOverloaded, func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})
	func() {
		defer func() { _ = recover() }()
		h(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	}()
	if got := a.inflight.inUse(); got != 0 {
		t.Fatalf("in use after panic = %d, want 0", got)
	}
}

// TestInflightGuardConcurrentNeverExceedsBound is the -race case.
func TestInflightGuardConcurrentNeverExceedsBound(t *testing.T) {
	const limit = 8
	a := newAPIAdmission(APILimits{MaxInflight: limit}, nil, metrics.New(), nil)
	var mu sync.Mutex
	current, peak := 0, 0
	h := a.guard(routeClassExecute, writeServerOverloaded, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		current++
		peak = max(peak, current)
		mu.Unlock()
		mu.Lock()
		current--
		mu.Unlock()
	})
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
		}()
	}
	wg.Wait()
	if peak > limit {
		t.Fatalf("peak concurrency %d exceeded bound %d", peak, limit)
	}
	if got := a.inflight.inUse(); got != 0 {
		t.Fatalf("in use after drain = %d, want 0", got)
	}
}

func TestInflightGuardDefaultsAndDisable(t *testing.T) {
	if got := newAPIAdmission(APILimits{}, nil, nil, nil).inflight.max; got != DefaultAPIMaxInflight {
		t.Fatalf("default bound = %d, want %d", got, DefaultAPIMaxInflight)
	}
	if got := newAPIAdmission(APILimits{}, nil, nil, nil).inflight.webhookMax; got != DefaultAPIMaxInflight/4 {
		t.Fatalf("default webhook sub-limit = %d, want %d", got, DefaultAPIMaxInflight/4)
	}
	a := newAPIAdmission(APILimits{MaxInflight: -1}, nil, nil, nil)
	if a.inflight != nil {
		t.Fatal("negative MaxInflight did not disable the bound")
	}
	called := false
	a.guard(routeClassExecute, writeServerOverloaded, func(http.ResponseWriter, *http.Request) { called = true })(
		httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	if !called {
		t.Fatal("disabled guard did not pass through")
	}
}

// TestInflightGuardWebhookSubLimit pins the webhook's own share of the bound: a
// webhook flood must be shed before it consumes the capacity authenticated
// execute work needs, while the global bound still applies on top.
func TestInflightGuardWebhookSubLimit(t *testing.T) {
	a := newAPIAdmission(APILimits{MaxInflight: 8}, nil, nil, nil)
	if got := a.inflight.webhookMax; got != 2 {
		t.Fatalf("webhookMax = %d, want 8/4 = 2", got)
	}

	var releases []func()
	for i := 0; i < 2; i++ {
		release, ok := a.acquire(routeClassWebhook)
		if !ok {
			t.Fatalf("webhook slot %d refused below its sub-limit", i)
		}
		releases = append(releases, release)
	}
	if _, ok := a.acquire(routeClassWebhook); ok {
		t.Fatal("webhook admitted past its sub-limit while the replica had room")
	}

	// The rest of the replica's capacity is still the execute class's.
	for i := 0; i < 6; i++ {
		release, ok := a.acquire(routeClassExecute)
		if !ok {
			t.Fatalf("execute slot %d refused below the replica bound (%d total held)", i, a.inflight.inUse())
		}
		releases = append(releases, release)
	}
	if _, ok := a.acquire(routeClassExecute); ok {
		t.Fatal("global bound admitted past the replica limit")
	}
	for _, release := range releases {
		release()
	}
	if got := a.inflight.inUse(); got != 0 {
		t.Fatalf("in use after release = %d, want 0", got)
	}
}

// TestInflightGuardWebhookSubLimitFloor keeps a tiny bound useful: a fractional
// quarter rounds down to nothing, so it floors at one slot.
func TestInflightGuardWebhookSubLimitFloor(t *testing.T) {
	for _, c := range []struct{ limit, want int }{{1, 1}, {3, 1}, {4, 1}, {5, 1}, {8, 2}} {
		got := newAPIAdmission(APILimits{MaxInflight: c.limit}, nil, nil, nil).inflight.webhookMax
		if got != c.want {
			t.Errorf("MaxInflight %d: webhookMax = %d, want %d", c.limit, got, c.want)
		}
	}
}
