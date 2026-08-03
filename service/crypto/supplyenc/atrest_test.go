package supplyenc

import (
	"bytes"
	"testing"
)

func testDEK() [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = byte(i * 7)
	}
	return k
}

func TestSealOpenRoundTrip(t *testing.T) {
	a := NewAtRest(testDEK())
	plain := []byte(`{"rules":[{"field":"authorization","action":"redact"}]}`)
	sealed, err := a.Seal(plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(sealed, []byte("authorization")) {
		t.Fatal("sealed output contains plaintext; nothing was encrypted")
	}
	opened, err := a.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(opened, plain) {
		t.Errorf("round trip changed the content")
	}
}

// 升级路径：加密开启之前写入的行是明文。如果 Open 对它们报错，升级瞬间
// 每一个既有 supply 都读不出来，门控会拒绝全部 activation。
func TestOpenPassesThroughPlaintext(t *testing.T) {
	a := NewAtRest(testDEK())
	plain := []byte(`{"rules":[]}`)
	opened, err := a.Open(plain)
	if err != nil {
		t.Fatalf("Open on pre-encryption plaintext: %v", err)
	}
	if !bytes.Equal(opened, plain) {
		t.Errorf("plaintext passthrough altered the content")
	}
}

// 错的 DEK 必须报错，不能返回垃圾字节：静默返回乱数据会让 wasm guest 拿到
// 无法解析的规则集，表现为「规则为空」而不是「解密失败」。
func TestOpenWithWrongKeyFails(t *testing.T) {
	sealed, err := NewAtRest(testDEK()).Seal([]byte("secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	var other [32]byte
	other[0] = 0xFF
	if _, err := NewAtRest(other).Open(sealed); err == nil {
		t.Fatal("Open succeeded with the wrong key")
	}
}

// 每次 Seal 用新 nonce，所以同样的明文产生不同的密文。这正是
// ContentHash 绝不能算在密文上的原因。
func TestSealIsNonDeterministic(t *testing.T) {
	a := NewAtRest(testDEK())
	plain := []byte("same input")
	first, err := a.Seal(plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	second, err := a.Seal(plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("two Seals of the same plaintext are byte-identical; the nonce is not random")
	}
}

func TestSealEmptyContent(t *testing.T) {
	a := NewAtRest(testDEK())
	sealed, err := a.Seal(nil)
	if err != nil {
		t.Fatalf("Seal(nil): %v", err)
	}
	opened, err := a.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(opened) != 0 {
		t.Errorf("empty content round-tripped to %d bytes", len(opened))
	}
}

// 结构性损坏的信封（截断导致 JSON 解析失败）不能被当作明文放行。IsEncrypted
// 已经判定这段数据自称是信封；一旦如此，解析失败就是损坏，不是「其实是明文」。
// 静默放行会把截断的密文当规则集喂给 wasm guest，表现为「规则为空」而不是
// 「解密失败」。
func TestOpenRejectsCorruptedEnvelope(t *testing.T) {
	a := NewAtRest(testDEK())
	sealed, err := a.Seal([]byte(`{"rules":[]}`))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if !IsEncrypted(sealed) {
		t.Fatalf("test fixture is not recognized as an envelope: %s", sealed)
	}
	corrupted := sealed[:len(sealed)-5]
	// Sanity check on the fixture: IsEncrypted's cheap prefix check must
	// still match the damaged data, which is exactly the scenario this test
	// targets (a payload that claims to be an envelope but fails to parse).
	if !IsEncrypted(corrupted) {
		t.Fatalf("test fixture setup invalid: truncated data no longer looks like an envelope")
	}
	if _, err := a.Open(corrupted); err == nil {
		t.Fatal("Open succeeded on a structurally corrupted envelope")
	}
}

// kid 匹配但原始密钥字节不对：GCM tag 校验必须失败，而不是静默返回垃圾字节
// 或走到 ErrUnknownKey 分支。两个独立随机生成的 Key 几乎必然拥有不同的 8
// 位十六进制 ID，所以已有的「错的 DEK」测试其实只覆盖了 ErrUnknownKey 分支；
// 这里手工构造一个 ID 相同但 Raw 不同的伪造 Key 来命中 tag 校验失败分支。
func TestOpenWithMatchingKeyIDButWrongRawFails(t *testing.T) {
	dek := testDEK()
	realKey := KeyFromBytes(dek)
	a := NewAtRest(dek)
	sealed, err := a.Seal([]byte("secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	var forgedRaw [32]byte
	for i := range forgedRaw {
		forgedRaw[i] = byte(i*7 + 1) // deliberately different from testDEK()
	}
	forgedKey := &Key{ID: realKey.ID, Raw: forgedRaw}
	if forgedKey.Raw == realKey.Raw {
		t.Fatalf("test fixture invalid: forged key has the same raw bytes as the real key")
	}
	forged := &AtRest{key: forgedKey, keyring: NewKeyring(forgedKey)}

	if _, err := forged.Open(sealed); err == nil {
		t.Fatal("Open succeeded with a key whose ID matches but whose raw bytes are wrong")
	}
}
