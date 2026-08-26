package control

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// A note on what is deliberately NOT added here. group_control_loop_test.go:
// 22-26 pins groupLeaseEngine, nodeLeaseEngine and nodeTimeoutCommitter against
// *engine.Engine, and subgraphLeaseEngine (group_control_loop.go:52-54) is
// missing from that list. It looks like the same gap, and it is not one:
//
//   - A rename or signature drift on *engine.Engine.BuildSubgraphLease is
//     already caught, just elsewhere. engine/lease.go:153 calls it internally
//     and engine/subgraph_lease_test.go calls it by name in a dozen places, so
//     the engine package stops building.
//   - A drift on this package's side of the interface is caught here.
//     dispatchSubgraphLease resolves it by type assertion, so an interface that
//     gains a method subgraphFakeEngine (lease_trace_carrier_test.go:216) does
//     not have makes that assertion fail and takes the tests through it red.
//
// Adding the pin would be consistency, not coverage, so it is left out rather
// than committed under a claim it cannot support.

// TestRedisAssignmentRoundTripPreservesUnitIdx pins the hand-rolled UnitIdx
// hop in redis_runner_directory_codec.go:96 and :124-129.
//
// The hop exists because engine.Task.UnitIdx carries `json:"-"` (engine/types.go:77),
// so it does not travel with the embedded Task field. The codec carries it in a
// separate *int and restores it on decode, mapping absence back to
// engine.UnitIdxUnknown so that "no unit index" stays distinguishable from
// "unit index 0".
//
// No test asserts the decoded value. redisDirectoryTestAssignment
// (redis_runner_directory_test.go:291-309) never sets Task.UnitIdx, so it is
// always the Go zero value 0 rather than the sentinel, and grepping UnitIdx
// across this package's tests finds no assertion on the decoded field at all.
//
// The three cases below are the three distinct behaviours, and they are
// distinct on purpose: 0 is a real index that must survive as 0, 3 is a real
// index that must survive as 3, and the sentinel must survive as the sentinel.
// A codec that always emits nil passes a test that only checks the sentinel; a
// codec that drops the absence mapping passes a test that only checks real
// indices.
//
// What breaks in production: a group task's UnitIdx is normally a real index.
// Send it through Redis and back — which happens on every control-plane restart
// and every re-registration replay, not on first dispatch — and a codec that
// loses it hands engine/group_exec.go a -1, whose bounds check reads that as
// "out of range or not a group". The task fails permanently and cannot be
// recovered, and it only ever happens after a restart, so first-dispatch tests
// and manual smoke runs never see it.
func TestRedisAssignmentRoundTripPreservesUnitIdx(t *testing.T) {
	cases := []struct {
		name        string
		unitIdx     int
		wantOnWire  bool
		wantDecoded int
	}{
		{name: "real index zero", unitIdx: 0, wantOnWire: true, wantDecoded: 0},
		{name: "real index", unitIdx: 3, wantOnWire: true, wantDecoded: 3},
		{name: "unknown sentinel", unitIdx: engine.UnitIdxUnknown, wantOnWire: false, wantDecoded: engine.UnitIdxUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := Assignment{
				AssignmentID: "assign-1",
				Task: engine.Task{
					ExecutionID: "exec-1",
					NodeName:    "map",
					Type:        engine.TaskTypeNodeBatch,
					UnitIdx:     tc.unitIdx,
				},
				Namespace: namespace.Default,
			}

			payload, err := marshalRedisAssignment(in)
			if err != nil {
				t.Fatalf("marshalRedisAssignment() error = %v", err)
			}

			// The wire shape is asserted directly, not only through the decode.
			// A codec that both wrote and read the wrong thing would round-trip
			// cleanly while being incompatible with anything already in Redis.
			var wire map[string]json.RawMessage
			if err := json.Unmarshal([]byte(payload), &wire); err != nil {
				t.Fatalf("decoding the payload as a generic object: %v", err)
			}
			if _, present := wire["unit_idx"]; present != tc.wantOnWire {
				t.Fatalf("unit_idx present on the wire = %v, want %v: the field must be "+
					"omitted for the unknown sentinel and present for every real index, "+
					"or absence on decode stops meaning what the decoder assumes it means",
					present, tc.wantOnWire)
			}

			out, err := unmarshalRedisAssignment(payload)
			if err != nil {
				t.Fatalf("unmarshalRedisAssignment() error = %v", err)
			}
			if out.Task.UnitIdx != tc.wantDecoded {
				t.Fatalf("decoded Task.UnitIdx = %d, want %d: engine.Task.UnitIdx is "+
					"json:\"-\", so it only survives Redis through the explicit hop in "+
					"this codec. Losing it turns a live batch into one that "+
					"engine/group_exec.go reads as out of range, permanently, and only "+
					"after a control-plane restart replays the assignment",
					out.Task.UnitIdx, tc.wantDecoded)
			}
		})
	}
}

// TestTokenFilePolicyIsReadEnforcedAndStillRequired pins auth.go:216-237, the
// token_file arm of resolveToken.
//
// The same 0o077 permission rule one level up — on the policy file itself —
// is pinned by TestFilePolicyStoreReloadRefusesInsecureFileMode
// (auth_test.go:169). The rule on the token file is not. PolicyEntry.TokenFile
// has no hits in any *_test.go in the repository; the TokenFile identifiers in
// cmd/server/main_test.go and auth_token_mappings_subject_test.go belong to a
// separate JSON token-mappings mechanism, not to this YAML field.
//
// Two different things can be removed here, and they fail differently:
//
//   - The permission check. Without it a token_file at 0644 loads without
//     complaint and the bearer token sits in plaintext readable by every other
//     user and process on the host. Nothing logs it; the runners authenticate
//     normally.
//   - The whole case arm. resolveToken then returns "" for a token_file entry,
//     so hasToken stays false. For an entry that has only a token_file, the
//     "at least one of token, token_file, mtls_subject is required" check at
//     auth.go:208 catches it. For an entry that pairs token_file with
//     mtls_subject — the shape a deployment uses when it wants both — nothing
//     catches it: the entry silently degrades to mTLS-subject-only, and any
//     client presenting the right certificate authenticates with any token at
//     all, or with none.
//
// The third sub-test is that second case, which is why it asserts a wrong
// token is refused while the peer CN matches, rather than only that the right
// token is accepted.
func TestTokenFilePolicyIsReadEnforcedAndStillRequired(t *testing.T) {
	const token = "s3cr3t-runner-token"

	writeTokenFile := func(t *testing.T, dir string, mode os.FileMode) string {
		t.Helper()
		path := filepath.Join(dir, "token")
		// A trailing newline on purpose: an operator writing this with `echo`
		// gets one, and resolveToken's TrimSpace is what makes that work.
		mustWriteFile(t, path, token+"\n", mode)
		return path
	}

	t.Run("a 0600 token file is read and its token authenticates", func(t *testing.T) {
		dir := t.TempDir()
		tokenPath := writeTokenFile(t, dir, 0o600)
		policyPath := filepath.Join(dir, "runners.yaml")
		mustWriteFile(t, policyPath, `version: 1
runners:
  - id_prefix: v1-
    token_file: `+tokenPath+`
    allowed_node_types: ["*"]
`, 0o600)

		store, err := NewFilePolicyStore(policyPath, false)
		if err != nil {
			t.Fatalf("NewFilePolicyStore() error = %v: an entry whose only credential "+
				"is a token_file must load", err)
		}
		if _, err := store.AuthenticateRegister("v1-a", token, TransportInfo{}); err != nil {
			t.Fatalf("AuthenticateRegister with the token from the file = %v, want nil: "+
				"the file's contents never reached the policy", err)
		}
		if _, err := store.AuthenticateRegister("v1-a", "not-the-token", TransportInfo{}); err == nil {
			t.Fatal("a wrong token was accepted: the entry is not actually bound to " +
				"the token file's contents")
		}
	})

	t.Run("a world-readable token file is refused", func(t *testing.T) {
		dir := t.TempDir()
		tokenPath := writeTokenFile(t, dir, 0o644)
		policyPath := filepath.Join(dir, "runners.yaml")
		mustWriteFile(t, policyPath, `version: 1
runners:
  - id_prefix: v1-
    token_file: `+tokenPath+`
    allowed_node_types: ["*"]
`, 0o600)

		_, err := NewFilePolicyStore(policyPath, false)
		if err == nil {
			t.Fatal("a token_file at mode 0644 was accepted: the bearer token is " +
				"readable by every other user on the host and nothing reports it")
		}
		if !strings.Contains(err.Error(), "world/group-readable") {
			t.Fatalf("error = %v, want it to name the permission problem: the operator "+
				"has to be able to tell this apart from a missing or malformed file", err)
		}
	})

	t.Run("token_file paired with mtls_subject still requires the token", func(t *testing.T) {
		dir := t.TempDir()
		tokenPath := writeTokenFile(t, dir, 0o600)
		policyPath := filepath.Join(dir, "runners.yaml")
		mustWriteFile(t, policyPath, `version: 1
runners:
  - id_prefix: v1-
    token_file: `+tokenPath+`
    mtls_subject: "CN=runner-prod"
    allowed_node_types: ["*"]
`, 0o600)

		store, err := NewFilePolicyStore(policyPath, false)
		if err != nil {
			t.Fatalf("NewFilePolicyStore() error = %v", err)
		}

		matching := TransportInfo{TLSPeerCN: "CN=runner-prod"}
		if _, err := store.AuthenticateRegister("v1-a", token, matching); err != nil {
			t.Fatalf("both credentials correct, err = %v, want nil", err)
		}
		if _, err := store.AuthenticateRegister("v1-a", "not-the-token", matching); err == nil {
			t.Fatal("the right certificate plus a wrong token authenticated: the " +
				"token_file half of this entry was dropped, so the policy has " +
				"silently degraded to mTLS-subject-only and the token is no longer " +
				"checked at all")
		}
		if _, err := store.AuthenticateRegister("v1-a", "", matching); err == nil {
			t.Fatal("the right certificate with no token at all authenticated")
		}
	})
}
