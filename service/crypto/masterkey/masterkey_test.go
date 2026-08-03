package masterkey

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validKeyB64() string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestLoadFromEnv(t *testing.T) {
	k, err := Load(validKeyB64(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if k == nil {
		t.Fatal("Load returned nil key with no error")
	}
}

// 长度校验是这个包存在的主要理由：一个 31 字节或 33 字节的 key 必须让进程
// 起不来，而不是被静默填充或截断成看起来能用的东西。
func TestLoadRejectsWrongLength(t *testing.T) {
	for _, n := range []int{16, 31, 33, 64} {
		raw := make([]byte, n)
		_, err := Load(base64.StdEncoding.EncodeToString(raw), "")
		if err == nil {
			t.Errorf("Load accepted a %d-byte key; only 32 is valid", n)
		}
	}
}

func TestLoadRejectsNonBase64(t *testing.T) {
	// 这正是「弱密钥」会长的样子：有人直接把口令填进环境变量。
	_, err := Load("changeme", "")
	if err == nil {
		t.Fatal("Load accepted a non-base64 value; a passphrase must not become a key")
	}
}

// Unlike TestLoadRejectsNonBase64 (which is actually caught by the length
// check, since "changeme" decodes to a valid-but-wrong-length 6 bytes), this
// exercises the decode-failure branch itself: '!' is outside the standard
// base64 alphabet, so DecodeString must fail before length is even checked.
func TestLoadRejectsInvalidBase64Chars(t *testing.T) {
	_, err := Load(strings.Repeat("a", 40)+"!", "")
	if err == nil {
		t.Fatal("Load accepted a value containing a non-base64 character")
	}
}

func TestLoadNotConfigured(t *testing.T) {
	_, err := Load("", "")
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("err = %v, want ErrNotConfigured", err)
	}
}

func TestLoadFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, []byte(validKeyB64()), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	if _, err := Load("", path); err != nil {
		t.Fatalf("Load from file: %v", err)
	}
}

// 与 --auth-tokens-file 同样的权限门槛：一个组可读的密钥文件等于密钥已泄漏
// 给同机的其他账号。
func TestLoadRejectsLoosePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, []byte(validKeyB64()), 0o644); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	_, err := Load("", path)
	if err == nil {
		t.Fatal("Load accepted a 0644 key file; want a permission error")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Errorf("error %q does not tell the operator how to fix it", err)
	}
}

func TestEnvTakesPrecedenceOverFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master.key")
	other := make([]byte, 32)
	for i := range other {
		other[i] = 0xAA
	}
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(other)), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	k, err := Load(validKeyB64(), path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := k.Derive("probe")
	want := mustLoad(t, validKeyB64()).Derive("probe")
	if got != want {
		t.Error("file value won over env; env must take precedence")
	}
}

// 派生必须按 info 分叉，否则「通用 KEK」的一处泄漏就等于全线沦陷，
// 而分用途派生正是接受通用命名的前提条件。
func TestDeriveIsDistinctPerInfo(t *testing.T) {
	k := mustLoad(t, validKeyB64())
	a := k.Derive("xflow-supply-content-v1")
	b := k.Derive("xflow-artifact-v1")
	if a == b {
		t.Fatal("two different info strings derived the same key")
	}
	if a == k.raw {
		t.Fatal("derived key equals the master key; HKDF did not run")
	}
}

func TestDeriveIsDeterministic(t *testing.T) {
	k := mustLoad(t, validKeyB64())
	if k.Derive("same") != k.Derive("same") {
		t.Fatal("Derive is not deterministic; stored data would become unreadable")
	}
}

func mustLoad(t *testing.T, b64 string) *Key {
	t.Helper()
	k, err := Load(b64, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return k
}

// A later task wires this package into cmd/server, where an accidental %+v
// on a config struct is plausible. fmt's reflection reaches unexported
// fields regardless of export status, so String/GoString are the only
// reliable guard; this asserts on the actual key bytes (in the various
// representations fmt would otherwise render them in), not on a substring
// we merely hope is absent.
func TestKeyRedactedInFmt(t *testing.T) {
	k := mustLoad(t, validKeyB64())
	rawBytes := k.raw[:]

	// These are exactly how fmt would render k.raw if String/GoString did
	// not intercept the call: decimal slice form, hex-joined form, and the
	// %#v Go-syntax form. If any of these leaked into the redacted output,
	// the guard would have failed to redact.
	leakForms := []string{
		fmt.Sprintf("%v", rawBytes),
		fmt.Sprintf("%d", rawBytes),
		fmt.Sprintf("%x", rawBytes),
		fmt.Sprintf("%#v", rawBytes),
	}

	outputs := []string{
		fmt.Sprintf("%v", k),
		fmt.Sprintf("%+v", k),
		fmt.Sprintf("%#v", k),
	}
	for _, out := range outputs {
		for _, leak := range leakForms {
			if strings.Contains(out, leak) {
				t.Fatalf("fmt output %q contains the raw key bytes in form %q", out, leak)
			}
		}
	}
}
