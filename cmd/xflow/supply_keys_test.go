package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/crypto/masterkey"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store/sqlstore"
)

func TestSupplyKeysStatusCommandRegistered(t *testing.T) {
	var out bytes.Buffer
	root := newRootCommand(&out)
	cmd, _, err := root.Find([]string{"supply", "keys", "status"})
	if err != nil {
		t.Fatalf("Find(supply keys status): %v", err)
	}
	if cmd == nil || cmd.Use != "status" {
		t.Fatalf("cmd = %+v, want the keys status subcommand", cmd)
	}
}

func TestRunSupplyKeysStatusRequiresMySQLDSN(t *testing.T) {
	var out bytes.Buffer
	err := runSupplyKeysStatus(context.Background(), &supplyKeysStatusOptions{out: &out})
	if err == nil {
		t.Fatal("err = nil, want an error when --mysql-dsn is not set")
	}
	if !strings.Contains(err.Error(), "mysql-dsn") {
		t.Fatalf("err = %v, want it to mention --mysql-dsn", err)
	}
}

func TestRunSupplyKeysStatusRequiresMasterKey(t *testing.T) {
	t.Setenv("XFLOW_MASTER_KEY", "")
	t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", "")
	var out bytes.Buffer
	opts := &supplyKeysStatusOptions{out: &out, mysqlDSN: "root:x@tcp(127.0.0.1:1)/xflow?parseTime=true"}
	err := runSupplyKeysStatus(context.Background(), opts)
	if err == nil {
		t.Fatal("err = nil, want an error when no current master key is configured")
	}
	if !strings.Contains(err.Error(), "master key") && !strings.Contains(err.Error(), "XFLOW_MASTER_KEY") {
		t.Fatalf("err = %v, want it to explain a current key is required", err)
	}
}

// buildResealKeys is shared by reseal and keys status. It must derive the
// reported fingerprints from the same DEKs the encryptors use, or the report
// would label rows with a kid that is not the one they were sealed under.
func TestBuildResealKeysReportsKeyFingerprints(t *testing.T) {
	currentB64 := supplyKeyB64ForCLI(0)
	previousB64 := supplyKeyB64ForCLI(64)

	t.Run("current only", func(t *testing.T) {
		t.Setenv("XFLOW_MASTER_KEY", currentB64)
		t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", "")
		keys, err := buildResealKeys("supply keys status", "", "")
		if err != nil {
			t.Fatalf("buildResealKeys: %v", err)
		}
		mk, err := masterkey.Load(currentB64, "")
		if err != nil {
			t.Fatalf("masterkey.Load: %v", err)
		}
		want := supplyenc.KeyFromBytes(mk.Derive(supplyenc.SupplyContentInfo)).ID
		if keys.currentKid != want {
			t.Errorf("currentKid = %q, want %q", keys.currentKid, want)
		}
		if got := keys.current.CurrentKeyID(); got != want {
			t.Errorf("current encryptor CurrentKeyID = %q, want %q", got, want)
		}
		if got := keys.atRest.CurrentKeyID(); got != want {
			t.Errorf("at-rest encryptor CurrentKeyID = %q, want %q", got, want)
		}
		if keys.previousKid != "" {
			t.Errorf("previousKid = %q, want empty outside a rotation window", keys.previousKid)
		}
	})

	t.Run("rotation window", func(t *testing.T) {
		t.Setenv("XFLOW_MASTER_KEY", currentB64)
		t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", previousB64)
		keys, err := buildResealKeys("supply keys status", "", "")
		if err != nil {
			t.Fatalf("buildResealKeys: %v", err)
		}
		mk, err := masterkey.Load(currentB64, "")
		if err != nil {
			t.Fatalf("masterkey.Load: %v", err)
		}
		prev, err := masterkey.LoadPrevious(previousB64, "", mk)
		if err != nil {
			t.Fatalf("masterkey.LoadPrevious: %v", err)
		}
		wantCurrent := supplyenc.KeyFromBytes(mk.Derive(supplyenc.SupplyContentInfo)).ID
		wantPrevious := supplyenc.KeyFromBytes(prev.Derive(supplyenc.SupplyContentInfo)).ID
		if keys.currentKid != wantCurrent {
			t.Errorf("currentKid = %q, want %q", keys.currentKid, wantCurrent)
		}
		if keys.previousKid != wantPrevious {
			t.Errorf("previousKid = %q, want %q", keys.previousKid, wantPrevious)
		}
		if keys.previousKid == keys.currentKid {
			t.Fatal("current and previous fingerprints are identical; the report would mislabel rows")
		}
		// The pass and the status scan must share the current key; only the
		// verification scan is current-only.
		if got := keys.atRest.CurrentKeyID(); got != wantCurrent {
			t.Errorf("at-rest encryptor CurrentKeyID = %q, want %q", got, wantCurrent)
		}
		if got := keys.current.CurrentKeyID(); got != wantCurrent {
			t.Errorf("verification encryptor CurrentKeyID = %q, want %q", got, wantCurrent)
		}
	})
}

func TestSupplyKeyStatusOutputJSON(t *testing.T) {
	out := newSupplyKeyStatusOutput(&sqlstore.SupplyKeyStatus{
		CurrentKeyID: "aaaaaaaa",
		Scanned:      3,
		ByKeyID:      map[string]int{"aaaaaaaa": 2, "bbbbbbbb": 1},
		NeedsReseal:  1,
	}, "bbbbbbbb")

	data, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{
		"current_key_id", "previous_key_id", "scanned", "by_key_id",
		"plaintext", "unparsable", "needs_reseal", "unreadable", "verified",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("output %s lacks %q: %s", data, key, data)
		}
	}
	if verified, _ := got["verified"].(bool); verified {
		t.Errorf("verified = true with NeedsReseal=1: %s", data)
	}
	if got["previous_key_id"] != "bbbbbbbb" {
		t.Errorf("previous_key_id = %v, want bbbbbbbb", got["previous_key_id"])
	}

	clean := newSupplyKeyStatusOutput(&sqlstore.SupplyKeyStatus{
		CurrentKeyID: "aaaaaaaa",
		Scanned:      1,
		ByKeyID:      map[string]int{"aaaaaaaa": 1},
	}, "")
	cleanJSON, err := json.Marshal(clean)
	if err != nil {
		t.Fatalf("marshal clean: %v", err)
	}
	if !clean.Verified {
		t.Errorf("verified = false for a clean report: %s", cleanJSON)
	}
	var cleanOut map[string]any
	if err := json.Unmarshal(cleanJSON, &cleanOut); err != nil {
		t.Fatalf("unmarshal clean: %v", err)
	}
	if _, ok := cleanOut["previous_key_id"]; ok {
		t.Errorf("clean output should omit previous_key_id: %s", cleanJSON)
	}
}

func TestSupplyResealOutputIncludesVerification(t *testing.T) {
	output := &supplyResealOutput{
		ResealReport: &sqlstore.ResealReport{Scanned: 4, Resealed: 1, Current: 3},
		Verification: newSupplyKeyStatusOutput(&sqlstore.SupplyKeyStatus{
			CurrentKeyID: "aaaaaaaa",
			Scanned:      4,
			ByKeyID:      map[string]int{"aaaaaaaa": 4},
		}, ""),
	}
	data, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["resealed"] != float64(1) {
		t.Errorf("resealed = %v, want 1 (the pass report must stay at the top level): %s", got["resealed"], data)
	}
	verification, ok := got["verification"].(map[string]any)
	if !ok {
		t.Fatalf("verification missing from %s", data)
	}
	if verification["verified"] != true {
		t.Errorf("verification.verified = %v, want true: %s", verification["verified"], data)
	}

	dryRun := &supplyResealOutput{ResealReport: &sqlstore.ResealReport{DryRun: true}}
	dryJSON, err := json.Marshal(dryRun)
	if err != nil {
		t.Fatalf("marshal dry run: %v", err)
	}
	var dryOut map[string]any
	if err := json.Unmarshal(dryJSON, &dryOut); err != nil {
		t.Fatalf("unmarshal dry run: %v", err)
	}
	if _, ok := dryOut["verification"]; ok {
		t.Errorf("dry run output must not claim verification: %s", dryJSON)
	}
}

func TestSupplyResealVerificationError(t *testing.T) {
	if err := supplyResealVerificationError(nil); err != nil {
		t.Errorf("nil verification returned %v, want nil", err)
	}
	if err := supplyResealVerificationError(&supplyKeyStatusOutput{Verified: true}); err != nil {
		t.Errorf("verified report returned %v, want nil", err)
	}
	err := supplyResealVerificationError(&supplyKeyStatusOutput{
		SupplyKeyStatus: &sqlstore.SupplyKeyStatus{NeedsReseal: 2, Unreadable: 1},
	})
	if err == nil {
		t.Fatal("err = nil, want a non-zero-exit-worthy error for a failed verification")
	}
	for _, want := range []string{"post-apply verification", "2 row(s) still need reseal", "1 row(s)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %q, want it to contain %q", err, want)
		}
	}
}
