package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store"
)

// supplyKeysTestDEKs returns three independent DEKs: the rotation's current and
// previous keys, plus a third that stands in for a key the configured keyring
// does not hold.
func supplyKeysTestDEKs() (current, previous, third [32]byte) {
	for i := range current {
		current[i] = byte(i + 1)
		previous[i] = byte(i + 101)
		third[i] = byte(i + 201)
	}
	return current, previous, third
}

func cleanupSupplyKeysNamespace(t *testing.T, db *gorm.DB, namespace string) {
	t.Helper()
	t.Cleanup(func() {
		if err := db.Exec("DELETE FROM xflow_supplies WHERE namespace = ?", namespace).Error; err != nil {
			t.Errorf("clean up supplies in %s: %v", namespace, err)
		}
	})
}

func supplyKeysTestNamespace(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// putSupplyForKeys writes one row with the given encryptor, or plaintext when
// encryption is nil.
func putSupplyForKeys(t *testing.T, db *gorm.DB, encryption SupplyEncryption, namespace, name, content string) {
	t.Helper()
	var provider *Provider
	if encryption == nil {
		provider = New(db)
	} else {
		provider = New(db, WithSupplyEncryption(encryption))
	}
	if _, err := provider.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: namespace,
		Name:      name,
		Content:   []byte(content),
	}, nil); err != nil {
		t.Fatalf("PutSupply %q: %v", name, err)
	}
}

// insertLyingSupplyRow writes a row whose envelope claims claimedKid while the
// ciphertext is sealed under sealedWith. It is the shape a kid census cannot
// distinguish from a healthy row: only opening it with the claimed key fails.
func insertLyingSupplyRow(t *testing.T, db *gorm.DB, namespace, name string, sealedWith *supplyenc.Key, claimedKid string, content []byte) {
	t.Helper()
	sealed, err := supplyenc.Encrypt(sealedWith, content)
	if err != nil {
		t.Fatalf("Encrypt lying row: %v", err)
	}
	// Declare the envelope fields in wire order. Unmarshalling into a map and
	// re-marshalling would sort the keys alphabetically, moving `v` off the
	// front; IsEncrypted sniffs that prefix, so the row would be classified as
	// plaintext instead of as the lying envelope this test needs.
	var env struct {
		Version   int    `json:"v"`
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
		Data      string `json:"data"`
	}
	if err := json.Unmarshal(sealed, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	env.KeyID = claimedKid
	lying, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal lied envelope: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO xflow_supplies (namespace, name, content, content_hash, revision) VALUES (?, ?, ?, ?, ?)`,
		namespace, name, b64Bytes(lying), store.ContentHash(content), 1,
	).Error; err != nil {
		t.Fatalf("insert lying row: %v", err)
	}
}

// TestInspectSupplyKeysClassifiesRows pins the complete, mutually exclusive
// partition the status command reports: current kid, previous kid, a kid the
// keyring does not hold, plaintext, and a row whose kid lies about its
// ciphertext key. It also pins the negative case a kid census gets wrong: the
// lying row is counted under the current kid AND reported unreadable.
func TestInspectSupplyKeysClassifiesRows(t *testing.T) {
	db := newSupplyResealTestDB(t)
	ctx := context.Background()
	namespace := supplyKeysTestNamespace(t, "supply-keys-classify")
	cleanupSupplyKeysNamespace(t, db, namespace)

	currentDEK, previousDEK, thirdDEK := supplyKeysTestDEKs()
	current := supplyenc.NewAtRest(currentDEK)
	previous := supplyenc.NewAtRest(previousDEK)
	third := supplyenc.NewAtRest(thirdDEK)
	rotating, err := supplyenc.NewAtRestWithPrevious(currentDEK, previousDEK)
	if err != nil {
		t.Fatalf("NewAtRestWithPrevious: %v", err)
	}

	putSupplyForKeys(t, db, current, namespace, "current-row", `{"row":"current"}`)
	putSupplyForKeys(t, db, previous, namespace, "previous-row", `{"row":"previous"}`)
	putSupplyForKeys(t, db, third, namespace, "unknown-row", `{"row":"unknown"}`)
	putSupplyForKeys(t, db, nil, namespace, "plaintext-row", `{"row":"plaintext"}`)
	insertLyingSupplyRow(t, db, namespace, "lying-row",
		supplyenc.KeyFromBytes(thirdDEK), current.CurrentKeyID(), []byte(`{"row":"lying"}`))

	// Capture one row's stored bytes to prove the scan is read-only.
	var contentBefore, revisionBefore string
	if err := db.Raw(
		"SELECT content, revision FROM xflow_supplies WHERE namespace = ? AND name = ?", namespace, "previous-row",
	).Row().Scan(&contentBefore, &revisionBefore); err != nil {
		t.Fatalf("read row before status: %v", err)
	}

	status, err := New(db).InspectSupplyKeys(ctx, rotating, SupplyKeyStatusOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("InspectSupplyKeys: %v", err)
	}

	if status.CurrentKeyID != current.CurrentKeyID() {
		t.Fatalf("CurrentKeyID = %q, want %q", status.CurrentKeyID, current.CurrentKeyID())
	}
	if status.Scanned != 5 {
		t.Fatalf("Scanned = %d, want 5", status.Scanned)
	}
	wantByKeyID := map[string]int{
		current.CurrentKeyID():  2, // the healthy current row and the lying row
		previous.CurrentKeyID(): 1,
		third.CurrentKeyID():    1,
	}
	if !reflect.DeepEqual(status.ByKeyID, wantByKeyID) {
		t.Fatalf("ByKeyID = %v, want %v", status.ByKeyID, wantByKeyID)
	}
	if status.Plaintext != 1 {
		t.Errorf("Plaintext = %d, want 1", status.Plaintext)
	}
	if status.Unparsable != 0 {
		t.Errorf("Unparsable = %d, want 0", status.Unparsable)
	}
	// previous-row, unknown-row and plaintext-row all still need a reseal; the
	// lying row claims the current kid, so it does not.
	if status.NeedsReseal != 3 {
		t.Errorf("NeedsReseal = %d, want 3", status.NeedsReseal)
	}
	// unknown-row cannot be opened (kid not held) and lying-row cannot be
	// opened (ciphertext under a key its kid does not name).
	if status.Unreadable != 2 {
		t.Errorf("Unreadable = %d, want 2", status.Unreadable)
	}
	if len(status.Issues) != 2 {
		t.Fatalf("Issues = %+v, want 2 entries", status.Issues)
	}
	reasons := make(map[string]string, len(status.Issues))
	for _, issue := range status.Issues {
		reasons[issue.Name] = issue.Reason
	}
	if !strings.Contains(reasons["unknown-row"], "no key matches kid") {
		t.Errorf("unknown-row reason = %q, want it to name the missing kid", reasons["unknown-row"])
	}
	if !strings.Contains(reasons["lying-row"], "decryption failed") {
		t.Errorf("lying-row reason = %q, want a decryption failure; a kid census would have called this row current", reasons["lying-row"])
	}
	if strings.Contains(reasons["lying-row"], "no key matches kid") {
		t.Errorf("lying-row was classified by its self-reported kid: %q", reasons["lying-row"])
	}

	var contentAfter, revisionAfter string
	if err := db.Raw(
		"SELECT content, revision FROM xflow_supplies WHERE namespace = ? AND name = ?", namespace, "previous-row",
	).Row().Scan(&contentAfter, &revisionAfter); err != nil {
		t.Fatalf("read row after status: %v", err)
	}
	if contentAfter != contentBefore || revisionAfter != revisionBefore {
		t.Fatal("InspectSupplyKeys modified a row; it must be read-only")
	}
}

// TestInspectSupplyKeysVerifiesCompletedReseal is the post-apply contract: once
// the pass has rewritten the old-kid row, a scan with the CURRENT key alone
// must both classify every row under the current kid and actually open each
// row with it.
func TestInspectSupplyKeysVerifiesCompletedReseal(t *testing.T) {
	db := newSupplyResealTestDB(t)
	ctx := context.Background()
	namespace := supplyKeysTestNamespace(t, "supply-keys-verify")
	cleanupSupplyKeysNamespace(t, db, namespace)

	currentDEK, previousDEK, _ := supplyKeysTestDEKs()
	current := supplyenc.NewAtRest(currentDEK)
	previous := supplyenc.NewAtRest(previousDEK)
	rotating, err := supplyenc.NewAtRestWithPrevious(currentDEK, previousDEK)
	if err != nil {
		t.Fatalf("NewAtRestWithPrevious: %v", err)
	}

	putSupplyForKeys(t, db, current, namespace, "current-row", `{"row":"current"}`)
	putSupplyForKeys(t, db, previous, namespace, "previous-row", `{"row":"previous"}`)

	// Before the pass, a current-only scan cannot read the old-key row: this is
	// exactly what makes failed=0 plus a kid census insufficient.
	before, err := New(db).InspectSupplyKeys(ctx, current, SupplyKeyStatusOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("InspectSupplyKeys before reseal: %v", err)
	}
	if before.NeedsReseal != 1 || before.Unreadable != 1 {
		t.Fatalf("before reseal: NeedsReseal = %d, Unreadable = %d, want 1 and 1", before.NeedsReseal, before.Unreadable)
	}

	rep, err := New(db, WithSupplyEncryption(rotating)).ResealSupplies(ctx, ResealOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("ResealSupplies: %v", err)
	}
	if rep.Resealed != 1 || rep.Failed != 0 {
		t.Fatalf("report = %+v, want exactly 1 resealed and 0 failed", rep)
	}

	after, err := New(db).InspectSupplyKeys(ctx, current, SupplyKeyStatusOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("InspectSupplyKeys after reseal: %v", err)
	}
	if after.NeedsReseal != 0 || after.Unreadable != 0 {
		t.Fatalf("after reseal: NeedsReseal = %d, Unreadable = %d, want 0 and 0", after.NeedsReseal, after.Unreadable)
	}
	if after.ByKeyID[current.CurrentKeyID()] != 2 {
		t.Errorf("ByKeyID = %v, want both rows under the current kid %q", after.ByKeyID, current.CurrentKeyID())
	}
	if after.ByKeyID[previous.CurrentKeyID()] != 0 {
		t.Errorf("ByKeyID = %v, want no rows left under the previous kid %q", after.ByKeyID, previous.CurrentKeyID())
	}

	// The rows are not merely labelled current; they still open to their
	// original plaintext through the normal read path.
	for name, want := range map[string]string{
		"current-row":  `{"row":"current"}`,
		"previous-row": `{"row":"previous"}`,
	} {
		got, err := New(db, WithSupplyEncryption(current)).GetSupply(ctx, namespace, name)
		if err != nil {
			t.Fatalf("GetSupply(%s): %v", name, err)
		}
		if string(got.Content) != want {
			t.Errorf("GetSupply(%s) = %q, want %q", name, got.Content, want)
		}
	}
}

// TestInspectSupplyKeysCatchesResidualPreviousRow simulates the sequencing
// hazard the runbook warns about: a lagging replica writes an old-key row
// after the pass has already scanned past it. The post-apply scan must report
// it as outstanding (and, read with the current key alone, unreadable) even
// though a scan with both keys can still open it.
func TestInspectSupplyKeysCatchesResidualPreviousRow(t *testing.T) {
	db := newSupplyResealTestDB(t)
	ctx := context.Background()
	namespace := supplyKeysTestNamespace(t, "supply-keys-lagging")
	cleanupSupplyKeysNamespace(t, db, namespace)

	currentDEK, previousDEK, _ := supplyKeysTestDEKs()
	current := supplyenc.NewAtRest(currentDEK)
	previous := supplyenc.NewAtRest(previousDEK)
	rotating, err := supplyenc.NewAtRestWithPrevious(currentDEK, previousDEK)
	if err != nil {
		t.Fatalf("NewAtRestWithPrevious: %v", err)
	}

	putSupplyForKeys(t, db, current, namespace, "current-row", `{"row":"current"}`)
	if _, err := New(db, WithSupplyEncryption(rotating)).ResealSupplies(ctx, ResealOptions{Namespace: namespace}); err != nil {
		t.Fatalf("ResealSupplies: %v", err)
	}
	// The lagging replica's write lands after the pass completed.
	putSupplyForKeys(t, db, previous, namespace, "lagging-row", `{"row":"lagging"}`)

	verification, err := New(db).InspectSupplyKeys(ctx, current, SupplyKeyStatusOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("InspectSupplyKeys (current only): %v", err)
	}
	if verification.NeedsReseal != 1 {
		t.Errorf("NeedsReseal = %d, want 1: the lagging row must keep the rotation open", verification.NeedsReseal)
	}
	if verification.Unreadable != 1 {
		t.Errorf("Unreadable = %d, want 1: the lagging row cannot be opened with the current key alone", verification.Unreadable)
	}

	status, err := New(db).InspectSupplyKeys(ctx, rotating, SupplyKeyStatusOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("InspectSupplyKeys (full keyring): %v", err)
	}
	if status.NeedsReseal != 1 {
		t.Errorf("status NeedsReseal = %d, want 1", status.NeedsReseal)
	}
	if status.Unreadable != 0 {
		t.Errorf("status Unreadable = %d, want 0: the previous key still reads the lagging row", status.Unreadable)
	}
	if status.ByKeyID[previous.CurrentKeyID()] != 1 {
		t.Errorf("status ByKeyID = %v, want one row under the previous kid %q", status.ByKeyID, previous.CurrentKeyID())
	}
}

// An encryptor that cannot identify keys must be refused: a report built
// without classification would be fabricated.
func TestInspectSupplyKeysRequiresKeyIdentification(t *testing.T) {
	status, err := New(nil).InspectSupplyKeys(context.Background(), &testSupplyEncryption{}, SupplyKeyStatusOptions{})
	if !errors.Is(err, ErrSupplyKeyInspectionNotConfigured) {
		t.Fatalf("err = %v, want ErrSupplyKeyInspectionNotConfigured", err)
	}
	if status != nil {
		t.Fatalf("status = %+v, want nil", status)
	}
}
