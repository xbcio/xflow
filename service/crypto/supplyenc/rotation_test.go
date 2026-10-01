package supplyenc

import (
	"bytes"
	"errors"
	"testing"
)

func otherDEK() [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = byte(0xA0 + i)
	}
	return k
}

// 轮换窗口的核心不变量：旧 key 封装的存量行在新 key 上线后仍然可读。
func TestAtRestWithPreviousOpensOldRows(t *testing.T) {
	oldRow, err := NewAtRest(testDEK()).Seal([]byte("legacy"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewAtRestWithPrevious(otherDEK(), testDEK())
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.Open(oldRow)
	if err != nil {
		t.Fatalf("Open old row during rotation: %v", err)
	}
	if !bytes.Equal(got, []byte("legacy")) {
		t.Fatal("old row content changed")
	}
}

// 新写入只能用 current：否则 previous 永远卸不掉。
func TestAtRestWithPreviousSealsUnderCurrentOnly(t *testing.T) {
	a, err := NewAtRestWithPrevious(otherDEK(), testDEK())
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := a.Seal([]byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	if a.NeedsReseal(sealed) {
		t.Fatal("freshly sealed row reported as needing reseal")
	}
	// Readable by the current key alone, i.e. after previous is removed.
	if _, err := NewAtRest(otherDEK()).Open(sealed); err != nil {
		t.Fatalf("current-only encryptor cannot open a new row: %v", err)
	}
	if _, err := NewAtRest(testDEK()).Open(sealed); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("previous-only encryptor err = %v, want ErrUnknownKey", err)
	}
}

func TestNewAtRestWithPreviousRejectsSameDEK(t *testing.T) {
	if _, err := NewAtRestWithPrevious(testDEK(), testDEK()); !errors.Is(err, ErrSameDEK) {
		t.Fatalf("err = %v, want ErrSameDEK", err)
	}
}

func TestNeedsReseal(t *testing.T) {
	oldRow, err := NewAtRest(testDEK()).Seal([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewAtRestWithPrevious(otherDEK(), testDEK())
	if err != nil {
		t.Fatal(err)
	}
	curRow, err := a.Seal([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		stored []byte
		want   bool
	}{
		{"previous kid", oldRow, true},
		{"current kid", curRow, false},
		// 加密启用前的明文行也要被重封，否则它会永远以明文留在库里。
		{"plaintext", []byte(`{"rules":[]}`), true},
		// 前缀像 envelope 但已损坏：必须交给 Open 报错，而不是当作“已是最新”跳过。
		{"damaged envelope", []byte(`{"v":1,"alg":"aes-256-gcm","kid":`), true},
	}
	for _, tc := range cases {
		if got := a.NeedsReseal(tc.stored); got != tc.want {
			t.Errorf("%s: NeedsReseal = %v, want %v", tc.name, got, tc.want)
		}
	}
	if a.CurrentKeyID() != KeyFromBytes(otherDEK()).ID {
		t.Error("CurrentKeyID is not the current DEK's fingerprint")
	}
}
