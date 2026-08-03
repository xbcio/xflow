package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
)

// ErrSupplyNotFound reports that the server has no content for a supply name.
// It is a legitimate cold-start state, not a failure of this runner.
var ErrSupplyNotFound = errors.New("supply: not found")

// supplyFetchTimeout bounds one content fetch. Generous relative to a heartbeat
// interval because it covers a full body read of up to the 1 MiB content cap,
// but bounded so a hung server cannot stall the activation loop.
const supplyFetchTimeout = 15 * time.Second

// maxSupplyResponseBytes caps what the runner will read from a supply response.
// It mirrors the server's own request-body cap; a response beyond it means the
// peer is not the server we think it is, so refusing is the safe reaction.
const maxSupplyResponseBytes = 1 << 20

// HTTPSupplyFetcher reads supply content from the control plane's
// GET /v1/supplies/{name}. The namespace is NOT part of the path or the body —
// the server derives it from the authenticated principal, so a runner cannot
// address another tenant's content even by accident.
type HTTPSupplyFetcher struct {
	BaseURL string
	Token   string
	Client  *http.Client
	// Keyring, when non-nil, enables encrypted supply fetching. The fetcher
	// sends Accept: application/x-xflow-encrypted and decrypts the response.
	Keyring *supplyenc.Keyring
}

var _ SupplyFetcher = (*HTTPSupplyFetcher)(nil)

func (f *HTTPSupplyFetcher) Fetch(ctx context.Context, name string) ([]byte, string, uint64, error) {
	if name == "" {
		return nil, "", 0, fmt.Errorf("supply fetch: empty name")
	}
	ctx, cancel := context.WithTimeout(ctx, supplyFetchTimeout)
	defer cancel()

	// PathEscape, not raw concatenation: a name is server-validated but this is
	// the boundary where a stray "/" or "?" would otherwise change the request.
	endpoint := f.BaseURL + "/v1/supplies/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", 0, fmt.Errorf("supply fetch: build request: %w", err)
	}
	if f.Token != "" {
		req.Header.Set("Authorization", "Bearer "+f.Token)
	}
	if f.Keyring.HasKeys() {
		req.Header.Set("Accept", "application/x-xflow-encrypted")
	}

	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		// Never wrap the raw error into anything user-facing; the caller only
		// logs it. The URL contains no credential (the token is a header).
		return nil, "", 0, fmt.Errorf("supply fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, "", 0, ErrSupplyNotFound
	default:
		// Do not echo the response body: it may carry a server error detail that
		// is not ours to surface.
		return nil, "", 0, fmt.Errorf("supply fetch: unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSupplyResponseBytes+1))
	if err != nil {
		return nil, "", 0, fmt.Errorf("supply fetch: read body: %w", err)
	}
	if len(body) > maxSupplyResponseBytes {
		return nil, "", 0, fmt.Errorf("supply fetch: content exceeds %d bytes", maxSupplyResponseBytes)
	}

	hash := resp.Header.Get("ETag")
	revision, _ := strconv.ParseUint(resp.Header.Get("X-Supply-Revision"), 10, 64)

	// Decrypt if the response is an encrypted envelope.
	if f.Keyring.HasKeys() && supplyenc.IsEncrypted(body) {
		plaintext, decErr := f.Keyring.Decrypt(body)
		if decErr != nil {
			return nil, "", 0, fmt.Errorf("supply fetch: decrypt: %w", decErr)
		}
		body = plaintext
	}

	return body, hash, revision, nil
}
