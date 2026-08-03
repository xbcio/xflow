//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

func atRestForTest() *supplyenc.AtRest {
	var dek [32]byte
	for i := range dek {
		dek[i] = byte(i + 1)
	}
	return supplyenc.NewAtRest(dek)
}

// uniqueSupplyName keeps parallel/repeat runs from colliding on the shared
// MySQL instance, matching how newExecutionID disambiguates in this package.
func uniqueSupplyName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func newSQLStoreProviderWithSupplyEncryption(t *testing.T, a *supplyenc.AtRest) *sqlstore.Provider {
	t.Helper()
	dsn := requireMySQL(t)
	p, err := mysqlstore.New(dsn, mysqlstore.WithSupplyEncryption(a))
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	return p
}

// rawSupplyContent reads the content column without going through supplyRepo,
// so it observes exactly what is on disk. Asserting on the repo's output would
// prove nothing: it decrypts.
func rawSupplyContent(t *testing.T, p *sqlstore.Provider, namespace, name string) []byte {
	t.Helper()
	row := p.DB().Raw(
		"SELECT content FROM xflow_supplies WHERE namespace = ? AND name = ?",
		namespace, name,
	).Row()
	var content []byte
	if err := row.Scan(&content); err != nil {
		t.Fatalf("read raw content column: %v", err)
	}
	return content
}

// 这是整个 task 的核心不变量。hash 一旦算在密文上，AES-GCM 的随机 nonce
// 会让同样的内容每次产生不同的 hash：幂等判重失效，且 runner 每次写入都
// 触发一次全量 wasm 池重建。
func TestContentHashStaysOnPlaintext(t *testing.T) {
	p := newSQLStoreProviderWithSupplyEncryption(t, atRestForTest())
	name := uniqueSupplyName(t, "hash")
	plain := []byte(`{"rules":[]}`)

	stored, err := p.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil)
	if err != nil {
		t.Fatalf("PutSupply: %v", err)
	}
	if stored.ContentHash != store.ContentHash(plain) {
		t.Fatalf("ContentHash = %q, want the hash of the PLAINTEXT %q",
			stored.ContentHash, store.ContentHash(plain))
	}
}

// 同一份内容写两次，hash 必须相同 —— 上一条测试的动态版本，直接钉死
// 「密文 hash」会造成的具体故障。
func TestSameContentTwiceYieldsSameHash(t *testing.T) {
	p := newSQLStoreProviderWithSupplyEncryption(t, atRestForTest())
	name := uniqueSupplyName(t, "twice")
	plain := []byte(`{"rules":[]}`)
	ctx := context.Background()

	first, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil)
	if err != nil {
		t.Fatalf("first PutSupply: %v", err)
	}
	second, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil)
	if err != nil {
		t.Fatalf("second PutSupply: %v", err)
	}
	if first.ContentHash != second.ContentHash {
		t.Fatalf("identical content produced different hashes (%q vs %q); "+
			"the hash is being computed on ciphertext, so every write now looks "+
			"like a change and rebuilds every consumer's pool",
			first.ContentHash, second.ContentHash)
	}
}

// 落库那一列必须真的是密文。这条是「加密到底有没有发生」的直接证据，
// 不能靠 round trip 通过来推断。
func TestStoredColumnIsCiphertext(t *testing.T) {
	p := newSQLStoreProviderWithSupplyEncryption(t, atRestForTest())
	name := uniqueSupplyName(t, "creds")
	plain := []byte(`{"password":"s3cr3t-sasl-pw"}`)

	if _, err := p.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	raw := rawSupplyContent(t, p, "ns", name)
	if bytes.Contains(raw, []byte("s3cr3t-sasl-pw")) {
		t.Fatal("the stored column still contains the plaintext credential")
	}
	if !supplyenc.IsEncrypted(raw) {
		t.Fatal("the stored column is not an encrypted envelope")
	}
}

func TestGetSupplyDecrypts(t *testing.T) {
	p := newSQLStoreProviderWithSupplyEncryption(t, atRestForTest())
	name := uniqueSupplyName(t, "get")
	plain := []byte(`{"rules":[{"tag":"pii"}]}`)
	ctx := context.Background()

	if _, err := p.PutSupply(ctx, &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}
	got, err := p.GetSupply(ctx, "ns", name)
	if err != nil {
		t.Fatalf("GetSupply: %v", err)
	}
	if !bytes.Equal(got.Content, plain) {
		t.Errorf("GetSupply returned %q, want the decrypted plaintext", got.Content)
	}
}

// 加密关闭时行为必须与加密引入之前完全一致，否则这个特性无法安全地
// 分阶段上线。
func TestEncryptionDisabledStoresPlaintext(t *testing.T) {
	p := newSQLStoreProvider(t) // 既有 helper，无加密
	name := uniqueSupplyName(t, "plain")
	plain := []byte(`{"rules":[]}`)

	if _, err := p.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}
	if raw := rawSupplyContent(t, p, "ns", name); !bytes.Equal(raw, plain) {
		t.Error("encryption is disabled but the stored bytes are not the plaintext")
	}
}

// 升级路径：加密开启之后，仍然要能读出开启之前写入的明文行。
func TestReadsPlaintextRowWrittenBeforeEncryption(t *testing.T) {
	name := uniqueSupplyName(t, "legacy")
	plain := []byte(`{"rules":["legacy"]}`)
	ctx := context.Background()

	plainProvider := newSQLStoreProvider(t)
	if _, err := plainProvider.PutSupply(ctx, &store.SupplyResource{
		Namespace: "ns", Name: name, Content: plain,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	// 同一个库，换成开启加密的 provider —— 模拟升级。
	encProvider := newSQLStoreProviderWithSupplyEncryption(t, atRestForTest())
	got, err := encProvider.GetSupply(ctx, "ns", name)
	if err != nil {
		t.Fatalf("GetSupply on a pre-encryption row: %v", err)
	}
	if !bytes.Equal(got.Content, plain) {
		t.Errorf("pre-encryption row read back as %q", got.Content)
	}
}

