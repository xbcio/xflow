package objectstore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// maxArtifactResponseBytes caps what HTTPStore will read from a single response.
// It must be kept in sync with store.MaxArtifactBytes (16 MiB). Defined here
// rather than imported to avoid the store->objectstore import cycle.
const maxArtifactResponseBytes = 16 << 20

// artifactFetchTimeout bounds a single artifact fetch. Generous because a full
// 16 MiB body may traverse a cross-cloud link, but bounded so a stalled server
// cannot block a runner indefinitely.
const artifactFetchTimeout = 60 * time.Second

// HTTPStore implements the read-only portion of Store by fetching artifacts from
// the server's GET /v1/artifacts/{digest} endpoint. It is the runner-side origin
// in a ReadThrough composition.
//
// Shape mirrors service/runner/supply_client.go's HTTPSupplyFetcher: same
// BaseURL/Token/Client triple, same Authorization header pattern, same
// "never echo response body in errors" discipline.
type HTTPStore struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

var _ Store = (*HTTPStore)(nil)

// PutObject is not supported: a runner never uploads artifacts to the server.
func (h *HTTPStore) PutObject(_ context.Context, _ string, _ io.Reader, _ int64, _ PutOptions) (*Object, error) {
	return nil, fmt.Errorf("objectstore/http: PutObject not supported (runner is read-only)")
}

// GetObject fetches the artifact identified by key from the server. Returns
// ErrNotFound when the server responds 404.
func (h *HTTPStore) GetObject(ctx context.Context, key string) (io.ReadCloser, *Object, error) {
	digest := digestFromKey(key)
	if digest == "" {
		return nil, nil, fmt.Errorf("objectstore/http: cannot derive digest from key %q", key)
	}

	ctx, cancel := context.WithTimeout(ctx, artifactFetchTimeout)
	// cancel is deferred only on error paths; on success the caller owns the
	// body and cancel is attached to limitedBody below.
	_ = cancel

	endpoint := h.BaseURL + "/v1/artifacts/" + url.PathEscape(digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("objectstore/http: build request: %w", err)
	}
	if h.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.Token)
	}

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("objectstore/http: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		_ = resp.Body.Close()
		cancel()
		return nil, nil, fmt.Errorf("objectstore/http: get %s: %w", digest, ErrNotFound)
	default:
		// Do not echo the response body: it may carry server internals.
		_ = resp.Body.Close()
		cancel()
		return nil, nil, fmt.Errorf("objectstore/http: unexpected status %d", resp.StatusCode)
	}

	obj := &Object{
		Key:          key,
		Size:         resp.ContentLength,
		ETag:         digest,
		ContentType:  resp.Header.Get("Content-Type"),
		LastModified: time.Now(),
	}

	// Wrap body with a limit to defend against a misbehaving server. The seed
	// carries one byte past the cap so that a body of exactly the cap reads
	// through cleanly; see limitedReadCloser.
	limited := &limitedReadCloser{
		rc:     resp.Body,
		remain: maxArtifactResponseBytes + 1,
		cancel: cancel,
	}
	return limited, obj, nil
}

// HeadObject issues an HTTP HEAD to check existence and size without
// downloading the body.
func (h *HTTPStore) HeadObject(ctx context.Context, key string) (*Object, error) {
	digest := digestFromKey(key)
	if digest == "" {
		return nil, fmt.Errorf("objectstore/http: cannot derive digest from key %q", key)
	}

	ctx, cancel := context.WithTimeout(ctx, artifactFetchTimeout)
	defer cancel()

	endpoint := h.BaseURL + "/v1/artifacts/" + url.PathEscape(digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("objectstore/http: build HEAD request: %w", err)
	}
	if h.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.Token)
	}

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("objectstore/http: HEAD: %w", err)
	}
	_ = resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, fmt.Errorf("objectstore/http: head %s: %w", digest, ErrNotFound)
	default:
		return nil, fmt.Errorf("objectstore/http: HEAD unexpected status %d", resp.StatusCode)
	}

	return &Object{
		Key:          key,
		Size:         resp.ContentLength,
		ETag:         digest,
		ContentType:  resp.Header.Get("Content-Type"),
		LastModified: time.Now(),
	}, nil
}

// limitedReadCloser wraps a body with a byte limit and owns a context cancel.
//
// remain is seeded with maxArtifactResponseBytes plus one detector byte, so the
// limit is "strictly more than the cap is too large" rather than "the cap
// itself is too large". The distinction is not academic: store.PutArtifact and
// sqlstore both admit a body of exactly MaxArtifactBytes, so treating that size
// as oversize here makes the largest artifact the server will store one that no
// runner can fetch back.
type limitedReadCloser struct {
	rc     io.ReadCloser
	remain int64
	cancel context.CancelFunc
}

func (l *limitedReadCloser) Read(p []byte) (int, error) {
	if l.remain <= 0 {
		return 0, errArtifactResponseTooLarge()
	}
	if int64(len(p)) > l.remain {
		p = p[:l.remain]
	}
	n, err := l.rc.Read(p)
	l.remain -= int64(n)
	if l.remain <= 0 {
		// The detector byte was delivered, so the body is strictly larger than
		// the cap. Fail on this read rather than waiting for the next one:
		// whether a reader issues another Read depends on when the transport
		// happens to report EOF, and the over-cap bytes are dropped rather than
		// handed to a caller that is about to see an error anyway.
		return 0, errArtifactResponseTooLarge()
	}
	return n, err
}

func errArtifactResponseTooLarge() error {
	return fmt.Errorf("objectstore/http: response exceeds %d bytes", maxArtifactResponseBytes)
}

func (l *limitedReadCloser) Close() error {
	l.cancel()
	return l.rc.Close()
}
