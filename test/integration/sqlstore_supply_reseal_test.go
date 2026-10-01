//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
)

func resealDEK(fill byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = fill + byte(i)
	}
	return k
}

// 每个测试独占一个 namespace：reseal 按 namespace 限定范围，才不会碰到共享库里
// 其他测试用别的 key 写的行（那些行在这里一定会被报为 failed）。
func resealNamespace() string {
	return fmt.Sprintf("reseal-%d", time.Now().UnixNano())
}

// 完整轮换生命周期：旧 key 写入 → 双 key 窗口仍可读 → reseal → 只留新 key 仍可读。
// 最后一步是这个功能存在的全部理由：reseal 之后 previous 必须可以卸掉。
func TestSupplyResealRotationLifecycle(t *testing.T) {
	ctx := context.Background()
	ns := resealNamespace()
	oldDEK, newDEK := resealDEK(1), resealDEK(100)

	before := newSQLStoreProviderWithSupplyEncryption(t, supplyenc.NewAtRest(oldDEK))
	written := map[string]*store.SupplyResource{}
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("s%d", i)
		rec, err := before.PutSupply(ctx, &store.SupplyResource{
			Namespace: ns, Name: name, Content: []byte(fmt.Sprintf(`{"rules":[%d]}`, i)),
		}, nil)
		if err != nil {
			t.Fatalf("PutSupply %s: %v", name, err)
		}
		written[name] = rec
	}

	rotating, err := supplyenc.NewAtRestWithPrevious(newDEK, oldDEK)
	if err != nil {
		t.Fatal(err)
	}
	during := newSQLStoreProviderWithSupplyEncryption(t, rotating)
	if _, err := during.GetSupply(ctx, ns, "s0"); err != nil {
		t.Fatalf("old row unreadable during the rotation window: %v", err)
	}

	dry, err := during.ResealSupplies(ctx, sqlstore.ResealOptions{Namespace: ns, DryRun: true, BatchSize: 2})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if dry.Resealed != 5 || dry.Failed != 0 {
		t.Fatalf("dry run report = %+v, want 5 to reseal, 0 failed", dry)
	}
	if !rotating.NeedsReseal(rawSupplyContent(t, during, ns, "s0")) {
		t.Fatal("dry run wrote to the store")
	}

	// BatchSize 2 over 5 rows forces the cursor across three pages.
	rep, err := during.ResealSupplies(ctx, sqlstore.ResealOptions{Namespace: ns, BatchSize: 2})
	if err != nil {
		t.Fatalf("reseal: %v", err)
	}
	if rep.Scanned != 5 || rep.Resealed != 5 || rep.Failed != 0 {
		t.Fatalf("reseal report = %+v, want 5 scanned/resealed, 0 failed", rep)
	}

	// Idempotent: a second pass finds everything current.
	again, err := during.ResealSupplies(ctx, sqlstore.ResealOptions{Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}
	if again.Resealed != 0 || again.Current != 5 {
		t.Fatalf("second pass = %+v, want 0 resealed, 5 already current", again)
	}

	after := newSQLStoreProviderWithSupplyEncryption(t, supplyenc.NewAtRest(newDEK))
	for name, orig := range written {
		got, err := after.GetSupply(ctx, ns, name)
		if err != nil {
			t.Fatalf("%s unreadable after previous key removed: %v", name, err)
		}
		// Plaintext, hash and revision are all unchanged: to a consumer a
		// reseal must not look like a content change.
		if string(got.Content) != string(orig.Content) || got.ContentHash != orig.ContentHash || got.Revision != orig.Revision {
			t.Fatalf("%s changed across reseal: got rev %d hash %s, want rev %d hash %s",
				name, got.Revision, got.ContentHash, orig.Revision, orig.ContentHash)
		}
	}
}

// 加密启用前写入的明文行也会被 reseal 加密，升级路径因此可以真正收尾。
func TestSupplyResealEncryptsPlaintextRows(t *testing.T) {
	ctx := context.Background()
	ns := resealNamespace()
	plain := newSQLStoreProviderWithSupplyEncryption(t, nil)
	if _, err := plain.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns, Name: "legacy", Content: []byte(`{"password":"pw"}`),
	}, nil); err != nil {
		t.Fatal(err)
	}
	a := supplyenc.NewAtRest(resealDEK(7))
	enc := newSQLStoreProviderWithSupplyEncryption(t, a)
	rep, err := enc.ResealSupplies(ctx, sqlstore.ResealOptions{Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Resealed != 1 || rep.Failed != 0 {
		t.Fatalf("report = %+v, want 1 resealed", rep)
	}
	if !supplyenc.IsEncrypted(rawSupplyContent(t, enc, ns, "legacy")) {
		t.Fatal("plaintext row still plaintext after reseal")
	}
}

// 用一把不认识的 key 封装的行：报告为 failed、原样保留、pass 继续——绝不能用新 key
// 把它“修好”，也不能中断其余行。
func TestSupplyResealReportsUnopenableRowsAndContinues(t *testing.T) {
	ctx := context.Background()
	ns := resealNamespace()
	stranger := newSQLStoreProviderWithSupplyEncryption(t, supplyenc.NewAtRest(resealDEK(200)))
	if _, err := stranger.PutSupply(ctx, &store.SupplyResource{Namespace: ns, Name: "a-orphan", Content: []byte("x")}, nil); err != nil {
		t.Fatal(err)
	}
	oldDEK, newDEK := resealDEK(1), resealDEK(100)
	old := newSQLStoreProviderWithSupplyEncryption(t, supplyenc.NewAtRest(oldDEK))
	if _, err := old.PutSupply(ctx, &store.SupplyResource{Namespace: ns, Name: "b-ok", Content: []byte("y")}, nil); err != nil {
		t.Fatal(err)
	}
	orphanBefore := rawSupplyContent(t, old, ns, "a-orphan")

	rotating, err := supplyenc.NewAtRestWithPrevious(newDEK, oldDEK)
	if err != nil {
		t.Fatal(err)
	}
	p := newSQLStoreProviderWithSupplyEncryption(t, rotating)
	rep, err := p.ResealSupplies(ctx, sqlstore.ResealOptions{Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed != 1 || rep.Resealed != 1 || len(rep.Failures) != 1 || rep.Failures[0].Name != "a-orphan" {
		t.Fatalf("report = %+v, want a-orphan failed and b-ok resealed", rep)
	}
	if string(rawSupplyContent(t, p, ns, "a-orphan")) != string(orphanBefore) {
		t.Fatal("unopenable row was modified")
	}
}

func TestSupplyResealRequiresEncryption(t *testing.T) {
	p := newSQLStoreProviderWithSupplyEncryption(t, nil)
	if _, err := p.ResealSupplies(context.Background(), sqlstore.ResealOptions{}); !errors.Is(err, sqlstore.ErrResealNotConfigured) {
		t.Fatalf("err = %v, want ErrResealNotConfigured", err)
	}
}

// 一行能用 previous key 正常打开（密文本身没坏），但打开后的明文跟落库时记录
// 的 content_hash 不一致——这与"打不开"是两条不同的失败路径：resealSupplyRow
// 必须在 Open 成功之后再做一次 hash 校验，把这类行也报 failed、原样保留，绝不能
// 用当前 key 把它重新封装成一个"看起来正常"的新信封。
func TestSupplyResealRefusesToLaunderHashMismatchedRow(t *testing.T) {
	ctx := context.Background()
	ns := resealNamespace()
	oldDEK, newDEK := resealDEK(1), resealDEK(100)

	old := newSQLStoreProviderWithSupplyEncryption(t, supplyenc.NewAtRest(oldDEK))
	name := "tampered"
	if _, err := old.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns, Name: name, Content: []byte(`{"rules":["orig"]}`),
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	// Overwrite content_hash to a value that will not match the plaintext once
	// decrypted. The envelope/ciphertext column is untouched, so Open still
	// succeeds; only the stored hash is wrong, forcing the hash-mismatch branch
	// rather than the open-failure branch exercised by
	// TestSupplyResealReportsUnopenableRowsAndContinues.
	if err := old.DB().Exec(
		"UPDATE xflow_supplies SET content_hash = ? WHERE namespace = ? AND name = ?",
		store.ContentHash([]byte("not the real plaintext")), ns, name,
	).Error; err != nil {
		t.Fatalf("tamper content_hash: %v", err)
	}
	beforeReseal := rawSupplyContent(t, old, ns, name)

	rotating, err := supplyenc.NewAtRestWithPrevious(newDEK, oldDEK)
	if err != nil {
		t.Fatal(err)
	}
	rotatingProvider := newSQLStoreProviderWithSupplyEncryption(t, rotating)
	rep, err := rotatingProvider.ResealSupplies(ctx, sqlstore.ResealOptions{Namespace: ns})
	if err != nil {
		t.Fatalf("reseal: %v", err)
	}
	if rep.Failed != 1 || rep.Resealed != 0 || len(rep.Failures) != 1 {
		t.Fatalf("report = %+v, want exactly 1 failed and 0 resealed", rep)
	}
	if rep.Failures[0].Name != name || rep.Failures[0].Reason == "" {
		t.Fatalf("failure entry = %+v, want name %q with a non-empty reason", rep.Failures[0], name)
	}
	if !bytes.Contains([]byte(rep.Failures[0].Reason), []byte("content hash")) {
		t.Fatalf("failure reason %q does not name a content-hash mismatch", rep.Failures[0].Reason)
	}

	// Row must be byte-for-byte untouched: still sealed under the old key,
	// still the same ciphertext, never laundered into a fresh envelope.
	afterReseal := rawSupplyContent(t, rotatingProvider, ns, name)
	if !bytes.Equal(beforeReseal, afterReseal) {
		t.Fatal("hash-mismatched row's stored content changed across reseal")
	}
	if rotating.NeedsReseal(afterReseal) == false {
		t.Fatal("tampered row now reports as already resealed, but it was left under the old key")
	}
}
