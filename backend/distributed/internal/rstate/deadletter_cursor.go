package rstate

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/types"
)

// cursorVersion is the opaque pagination cursor format version. A mismatch
// (e.g. a future format change) is rejected as an expired/invalid cursor.
const cursorVersion = 1

// deadLetterCursorPayload is the signed payload embedded in an opaque
// ListDeadLetters cursor. Score+member together form a stable (score, member)
// resume point so score-then-member ordering is preserved across pages even
// when multiple entries share the same score (same-ms dead-lettering).
type deadLetterCursorPayload struct {
	Version int                `json:"v"`
	ExecID  types.ExecutionID `json:"e"`
	Score   float64            `json:"s"`
	Member  string             `json:"m"`
}

// ErrCursorExpired is returned when an opaque cursor cannot be decoded or
// verified: it is tampered, malformed, belongs to another execution, uses an
// unknown version, or was issued by a previous process (whose signing key is
// gone). Callers must restart the listing from the first page.
var ErrCursorExpired = errors.New("dead-letter cursor expired or invalid")

// newCursorSigningKey generates a 32-byte process-local random key for HMAC-
// signing opaque cursors. Failing to read system randomness is fatal: a weak
// key would let an attacker forge cursors, so we refuse to start instead.
func newCursorSigningKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("rstate: generate cursor signing key: %v", err))
	}
	return key
}

// encodeDeadLetterCursor signs and encodes a (score, member) resume point for
// one execution into an opaque cursor token. The token is base64url payload +
// base64url HMAC-SHA256 signature, joined by ".". The execution id is bound
// into the signed payload so a cursor from one execution cannot be replayed
// against another.
func (s *Store) encodeDeadLetterCursor(execID types.ExecutionID, score float64, member string) string {
	payload := deadLetterCursorPayload{Version: cursorVersion, ExecID: execID, Score: score, Member: member}
	raw, _ := json.Marshal(payload)
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, s.cursorKey)
	mac.Write([]byte(encoded))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("v%d.%s.%s", cursorVersion, encoded, sig)
}

// decodeDeadLetterCursor verifies an opaque cursor's signature, version, and
// execution binding, returning the (score, member) resume point. Any failure
// (tampered, wrong exec, unknown version, stale signing key from a prior
// process) yields ErrCursorExpired so the caller restarts the listing.
func (s *Store) decodeDeadLetterCursor(cursor string, execID types.ExecutionID) (float64, string, error) {
	if cursor == "" {
		return 0, "", nil
	}
	parts := strings.SplitN(cursor, ".", 3)
	if len(parts) != 3 || parts[0] != fmt.Sprintf("v%d", cursorVersion) {
		return 0, "", ErrCursorExpired
	}
	encoded := parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return 0, "", ErrCursorExpired
	}
	mac := hmac.New(sha256.New, s.cursorKey)
	mac.Write([]byte(encoded))
	if !hmac.Equal(mac.Sum(nil), sig) {
		return 0, "", ErrCursorExpired
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return 0, "", ErrCursorExpired
	}
	var payload deadLetterCursorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, "", ErrCursorExpired
	}
	if payload.Version != cursorVersion || payload.ExecID != execID {
		return 0, "", ErrCursorExpired
	}
	return payload.Score, payload.Member, nil
}
