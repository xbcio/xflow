package masterkey

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func keyB64(fill byte) string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = fill + byte(i)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// 稳态下不配置 previous 是常态，必须是 ErrNotConfigured 而不是错误。
func TestLoadPreviousNotConfigured(t *testing.T) {
	cur := mustLoad(t, keyB64(0))
	if _, err := LoadPrevious("", "", cur); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("err = %v, want ErrNotConfigured", err)
	}
}

func TestLoadPreviousDerivesDistinctSubkey(t *testing.T) {
	cur := mustLoad(t, keyB64(0))
	prev, err := LoadPrevious(keyB64(100), "", cur)
	if err != nil {
		t.Fatalf("LoadPrevious: %v", err)
	}
	if prev.Derive("x") == cur.Derive("x") {
		t.Fatal("previous and current derived the same subkey")
	}
	if prev.Derive("x") != mustLoad(t, keyB64(100)).Derive("x") {
		t.Fatal("previous key is not the configured value")
	}
}

// 新旧两个槽位填了同一个值：reseal 会“成功”但什么也没换，必须拒绝。
func TestLoadPreviousRejectsEqualToCurrent(t *testing.T) {
	cur := mustLoad(t, keyB64(0))
	if _, err := LoadPrevious(keyB64(0), "", cur); !errors.Is(err, ErrPreviousEqualsCurrent) {
		t.Fatalf("err = %v, want ErrPreviousEqualsCurrent", err)
	}
}

func TestLoadPreviousRequiresCurrent(t *testing.T) {
	if _, err := LoadPrevious(keyB64(0), "", nil); err == nil || errors.Is(err, ErrNotConfigured) {
		t.Fatalf("err = %v, want a hard error for previous-without-current", err)
	}
	if _, err := LoadPrevious("", "", nil); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("err = %v, want ErrNotConfigured", err)
	}
}

// 校验与 Load 完全一致：错误长度、宽松权限都拒绝，且错误信息不回显密钥值。
func TestLoadPreviousAppliesLoadValidation(t *testing.T) {
	cur := mustLoad(t, keyB64(0))

	short := base64.StdEncoding.EncodeToString(make([]byte, 31))
	_, err := LoadPrevious(short, "", cur)
	if err == nil {
		t.Fatal("accepted a 31-byte previous key")
	}
	if strings.Contains(err.Error(), short) {
		t.Fatal("error echoes the key value")
	}
	if !strings.Contains(err.Error(), "previous") {
		t.Fatalf("error %q does not say it is about the previous key", err)
	}

	path := filepath.Join(t.TempDir(), "prev.key")
	if err := os.WriteFile(path, []byte(keyB64(50)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrevious("", path, cur); err == nil {
		t.Fatal("accepted a world-readable previous key file")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrevious("", path, cur); err != nil {
		t.Fatalf("0600 previous key file rejected: %v", err)
	}
}
