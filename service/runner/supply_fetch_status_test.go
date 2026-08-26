package runner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// TestSupplyFetchOnAnUnexpectedStatusNamesItAndEchoesNothing drives
// supply_client.go's default: arm (:82-86), the one the existing five tests
// (OK, NotFound, PathEscapes, BodyTooLarge, EmptyName) never construct a
// response for.
//
// The comment on that arm says "Do not echo the response body: it may carry a
// server error detail that is not ours to surface." I checked the claim before
// pinning it rather than assuming a comment that says redact does redact —
// io.ReadAll is at :88, after the switch, and the default arm returns at :85,
// so the body genuinely is never read. That makes this the opposite of the
// "名叫 redact 但没 redact" shape, and worth keeping that way: the runner logs
// this error, and a control-plane 500 body can carry a DSN or a stack.
//
// The two assertions pull against each other on purpose. The status number must
// be present, because without it an operator staring at a runner log cannot
// tell a misrouted request from an unauthorized one from a server fault. The
// body must not be, at all. Satisfying only one of them is easy; a change that
// starts wrapping the response for the sake of a better message fails the
// second, and a change that reduces the message to a bare "supply fetch failed"
// fails the first.
//
// Measured. Two mutations of the default arm, each run against all six packages
// that depend on service/runner, and each also run with this file removed:
//
//   - the message reduced to "supply fetch failed": all five sub-tests go red;
//     without this file the whole scope stays green.
//   - the arm returning ErrSupplyNotFound instead: all five go red on the
//     sentinel assertion; without this file the whole scope stays green.
//
// Nothing anywhere in the module distinguished a transport fault from an
// authoritative absence before this.
func TestSupplyFetchOnAnUnexpectedStatusNamesItAndEchoesNothing(t *testing.T) {
	// Every non-200/404 status the control plane can realistically produce,
	// including the two that a single-case test would most likely miss: a 401
	// from an expired runner token and a 503 from a draining server.
	for _, status := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
	} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			const detail = "supply store dsn=xflow:hunter2@tcp(10.0.0.7:3306)/xflow"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(detail))
			}))
			defer srv.Close()

			f := &HTTPSupplyFetcher{BaseURL: srv.URL, Client: srv.Client()}
			content, hash, revision, err := f.Fetch(context.Background(), "shared-rules")
			if err == nil {
				t.Fatalf("Fetch() on %d returned no error", status)
			}
			if errors.Is(err, ErrSupplyNotFound) {
				t.Fatalf("Fetch() on %d = ErrSupplyNotFound: only a real 404 may "+
					"produce that sentinel, since the supply gate treats it as "+
					"an authoritative absence rather than a transport fault", status)
			}
			if !strings.Contains(err.Error(), strconv.Itoa(status)) {
				t.Fatalf("Fetch() error = %v, want it to name status %d: this "+
					"string is all an operator gets in the runner log, and "+
					"401-vs-503 is the difference between a rotated token and "+
					"a draining server", err, status)
			}
			if strings.Contains(err.Error(), detail) || strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("Fetch() error echoed the response body: %v", err)
			}
			// Nothing may be handed back alongside the error either.
			if content != nil || hash != "" || revision != 0 {
				t.Fatalf("Fetch() on %d returned content=%q hash=%q revision=%d "+
					"with a non-nil error; a caller that checks the value before "+
					"the error would treat a server fault as a supply update",
					status, content, hash, revision)
			}
		})
	}
}
