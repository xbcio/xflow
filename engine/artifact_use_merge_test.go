package engine

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestBatchResultCarriesTheHostsArtifactList(t *testing.T) {
	uses := []types.ArtifactUse{
		{Node: "clean", Digest: "sha256:bb"},
		{Node: "decode", Digest: "sha256:aa"},
	}
	data, err := BatchResultForCommit([]BatchItemResult{{Index: 0, Data: map[string]any{"ok": true}}}, false, uses)
	if err != nil {
		t.Fatalf("BatchResultForCommit: %v", err)
	}
	want := []any{
		map[string]any{"node": "clean", "digest": "sha256:bb"},
		map[string]any{"node": "decode", "digest": "sha256:aa"},
	}
	if got := data["artifacts"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("data[artifacts] = %#v, want %#v", got, want)
	}
}

// A batch that used no artifact must not carry an empty key: the map node's
// output is what downstream reads, and an always-present empty list makes
// "this execution used no artifact" indistinguishable from "this build does
// not report artifacts".
func TestBatchResultOmitsArtifactsWhenThereAreNone(t *testing.T) {
	data, err := BatchResultForCommit([]BatchItemResult{{Index: 0, Data: map[string]any{"ok": true}}}, false, nil)
	if err != nil {
		t.Fatalf("BatchResultForCommit: %v", err)
	}
	if _, present := data["artifacts"]; present {
		t.Fatalf("data has an artifacts key with no uses: %#v", data)
	}
}

// batch_size is an operational knob, not semantics: the same items split across
// two batches must report the same artifact set as one batch would.
func TestMergeBatchArtifactsUnionsAcrossBatchesAndDedupes(t *testing.T) {
	batches := []map[string]any{
		{"items": []any{}, "artifacts": []any{
			map[string]any{"node": "decode", "digest": "sha256:aa"},
			map[string]any{"node": "clean", "digest": "sha256:bb"},
		}},
		{"items": []any{}, "artifacts": []any{
			map[string]any{"node": "decode", "digest": "sha256:aa"}, // same as batch 0
			map[string]any{"node": "clean", "digest": "sha256:cc"},  // a hot-swapped artifact
		}},
		{"items": []any{}}, // a batch that used no artifact at all
		nil,                // a batch slot that never reported
	}
	got := mergeBatchArtifacts(batches)
	want := []types.ArtifactUse{
		{Node: "clean", Digest: "sha256:bb"},
		{Node: "clean", Digest: "sha256:cc"},
		{Node: "decode", Digest: "sha256:aa"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeBatchArtifacts = %#v, want %#v", got, want)
	}
	if got := mergeBatchArtifacts([]map[string]any{{"items": []any{}}}); got != nil {
		t.Fatalf("mergeBatchArtifacts with no uses = %#v, want nil", got)
	}
}

// The guest's return value lands inside results[i]; the top-level key is built
// by host code from the collector. This asserts the two do not meet: a batch
// whose item data carries a forged "artifacts" key leaves the top-level list
// alone, and the forgery stays visible where it landed (which is what proves
// the fixture really did forge something).
func TestForgedArtifactsInItemDataDoNotReachTheTopLevel(t *testing.T) {
	forged := []any{map[string]any{"node": "decode", "digest": "sha256:ATTACKER"}}

	t.Run("host_has_real_uses", func(t *testing.T) {
		data, err := BatchResultForCommit(
			[]BatchItemResult{{Index: 0, Data: map[string]any{"artifacts": forged}}},
			false,
			[]types.ArtifactUse{{Node: "decode", Digest: "sha256:real"}},
		)
		if err != nil {
			t.Fatalf("BatchResultForCommit: %v", err)
		}
		want := []any{map[string]any{"node": "decode", "digest": "sha256:real"}}
		if got := data["artifacts"]; !reflect.DeepEqual(got, want) {
			t.Fatalf("top-level artifacts = %#v, want %#v", got, want)
		}
		items, _ := data["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("items = %#v, want one entry", items)
		}
		item, _ := items[0].(map[string]any)
		if !reflect.DeepEqual(item["artifacts"], forged) {
			t.Fatalf("the fixture's forgery vanished from results[0] (%#v) — the test is not testing anything", item)
		}
	})

	// This is the case the first sub-test cannot cover, and it is the one that
	// actually matters in production: an inline-code map node has no
	// artifact_digest, so the script node never calls Record and `uses` is
	// empty. The host's assignment is `if list := ...; list != nil { ... }` --
	// when uses is empty that condition is FALSE, so the branch that would
	// overwrite a forged value never runs at all. The first sub-test proves
	// "the host's value wins"; it cannot prove "the host still doesn't lose
	// when it has no value of its own" -- a forged key could slip through
	// exactly because nothing ran to stop it. That is a different failure
	// mode and needs its own assertion.
	t.Run("host_has_no_uses", func(t *testing.T) {
		data, err := BatchResultForCommit(
			[]BatchItemResult{{Index: 0, Data: map[string]any{"artifacts": forged}}},
			false,
			nil,
		)
		if err != nil {
			t.Fatalf("BatchResultForCommit: %v", err)
		}
		// Absence, not nil: a present-but-nil key and a missing key are two
		// different things, and Task 1's ArtifactUsesAsData contract is "no
		// uses, no key" -- so the assertion has to match that contract, not
		// merely "falsy".
		if _, present := data["artifacts"]; present {
			t.Fatalf("data has a top-level artifacts key with no host uses: %#v", data)
		}
		// Without this, the assertion above would pass even if the fixture's
		// forgery never landed in items[0] to begin with -- the same guard
		// the first sub-test relies on.
		items, _ := data["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("items = %#v, want one entry", items)
		}
		item, _ := items[0].(map[string]any)
		if !reflect.DeepEqual(item["artifacts"], forged) {
			t.Fatalf("the fixture's forgery vanished from results[0] (%#v) — the test is not testing anything", item)
		}
	})
}
