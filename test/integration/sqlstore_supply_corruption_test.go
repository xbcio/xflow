//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/store"
)

// Encrypted-path case: supplyenc.Open decides "this is pre-encryption
// plaintext" purely from a cheap prefix check (IsEncrypted). If the stored
// bytes are corrupted in a way that destroys that prefix, Open hands back the
// mangled bytes as "plaintext" with a nil error — undetectable inside
// supplyenc. GetSupply must catch this using content_hash, which is always
// the hash of the original plaintext and therefore will not match the
// corrupted bytes.
func TestGetSupplyDetectsCorruptedContent(t *testing.T) {
	p := newSQLStoreProviderWithSupplyEncryption(t, atRestForTest())
	name := uniqueSupplyName(t, "corrupt")
	plain := []byte(`{"rules":[{"tag":"pii"}]}`)
	ctx := context.Background()

	if _, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	// Corrupt the stored column directly, bypassing the repo. Front-truncate
	// far enough that IsEncrypted's prefix check no longer recognizes an
	// envelope, so Open takes the "it's plaintext" passthrough path and
	// returns the garbage bytes with a nil error.
	if err := p.DB().Exec(
		"UPDATE xflow_supplies SET content = 'garbage' WHERE namespace = ? AND name = ?",
		"ns", name,
	).Error; err != nil {
		t.Fatalf("corrupt stored content: %v", err)
	}

	if _, err := p.GetSupply(ctx, "ns", name); err == nil {
		t.Fatal("GetSupply succeeded on a row whose content does not match its stored content_hash")
	}
}

// Unencrypted-path case: the content_hash check is not encryption-specific.
// A deployment that never enables encryption still writes content_hash on
// every PutSupply, and a row damaged afterwards (truncation, a bad migration,
// a direct SQL edit) must still be rejected on read — there is no atRest to
// gate this on, and gating it would mean silently serving corrupted content
// whenever encryption is off.
func TestGetSupplyDetectsCorruptedContentWithoutEncryption(t *testing.T) {
	p := newSQLStoreProvider(t) // no atRest: encryption disabled
	name := uniqueSupplyName(t, "corrupt-plain")
	plain := []byte(`{"rules":[{"tag":"pii"}]}`)
	ctx := context.Background()

	if _, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	// Corrupt the stored column directly, bypassing the repo, so content no
	// longer matches the content_hash recorded alongside it.
	if err := p.DB().Exec(
		"UPDATE xflow_supplies SET content = 'garbage' WHERE namespace = ? AND name = ?",
		"ns", name,
	).Error; err != nil {
		t.Fatalf("corrupt stored content: %v", err)
	}

	if _, err := p.GetSupply(ctx, "ns", name); err == nil {
		t.Fatal("GetSupply succeeded on an unencrypted row whose content does not match its stored content_hash")
	}
}

