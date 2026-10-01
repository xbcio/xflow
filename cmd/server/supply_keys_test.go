package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/crypto/masterkey"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
)

func supplyKeyB64(fill byte) string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = fill + byte(i)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestLoadSupplyAtRestNoneConfigured(t *testing.T) {
	var warn bytes.Buffer
	a, err := loadSupplyAtRest(supplyKeyInputs{}, &warn)
	if err != nil {
		t.Fatalf("loadSupplyAtRest: %v", err)
	}
	if a != nil {
		t.Fatalf("a = %v, want nil", a)
	}
	if warn.Len() != 0 {
		t.Fatalf("warn = %q, want empty", warn.String())
	}
}

func TestLoadSupplyAtRestCurrentOnly(t *testing.T) {
	var warn bytes.Buffer
	a, err := loadSupplyAtRest(supplyKeyInputs{currentEnv: supplyKeyB64(0)}, &warn)
	if err != nil {
		t.Fatalf("loadSupplyAtRest: %v", err)
	}
	if a == nil {
		t.Fatal("a = nil, want a configured AtRest")
	}
	if warn.Len() != 0 {
		t.Fatalf("warn = %q, want empty (no rotation window without a previous key)", warn.String())
	}
}

func TestLoadSupplyAtRestCurrentAndPrevious(t *testing.T) {
	var warn bytes.Buffer
	in := supplyKeyInputs{
		currentEnv:  supplyKeyB64(0),
		previousEnv: supplyKeyB64(100),
	}
	a, err := loadSupplyAtRest(in, &warn)
	if err != nil {
		t.Fatalf("loadSupplyAtRest: %v", err)
	}
	if a == nil {
		t.Fatal("a = nil, want a configured AtRest")
	}

	// Warning must name the current key ID and point at the reseal command.
	if !strings.Contains(warn.String(), a.CurrentKeyID()) {
		t.Fatalf("warning %q does not mention kid %q", warn.String(), a.CurrentKeyID())
	}
	if !strings.Contains(warn.String(), "xflow supply reseal") {
		t.Fatalf("warning %q does not mention `xflow supply reseal`", warn.String())
	}

	// Rows sealed under the previous DEK must still open through the
	// returned AtRest (that is the whole point of the rotation window).
	prevKey, err := masterkey.Load(in.previousEnv, "")
	if err != nil {
		t.Fatalf("load previous key for fixture: %v", err)
	}
	prevAtRest := supplyenc.NewAtRest(prevKey.Derive(supplyenc.SupplyContentInfo))
	sealedByPrevious, err := prevAtRest.Seal([]byte(`{"rules":[]}`))
	if err != nil {
		t.Fatalf("seal fixture under previous key: %v", err)
	}
	opened, err := a.Open(sealedByPrevious)
	if err != nil {
		t.Fatalf("Open row sealed under previous key: %v", err)
	}
	if string(opened) != `{"rules":[]}` {
		t.Fatalf("opened = %q, want the original plaintext", opened)
	}
}

func TestLoadSupplyAtRestPreviousEqualsCurrentIsError(t *testing.T) {
	same := supplyKeyB64(0)
	_, err := loadSupplyAtRest(supplyKeyInputs{currentEnv: same, previousEnv: same}, nil)
	if !errors.Is(err, masterkey.ErrPreviousEqualsCurrent) {
		t.Fatalf("err = %v, want ErrPreviousEqualsCurrent", err)
	}
}

func TestLoadSupplyAtRestPreviousWithoutCurrentIsError(t *testing.T) {
	_, err := loadSupplyAtRest(supplyKeyInputs{previousEnv: supplyKeyB64(0)}, nil)
	if err == nil {
		t.Fatal("err = nil, want an error for previous-without-current")
	}
	if errors.Is(err, masterkey.ErrNotConfigured) {
		t.Fatalf("err = %v, want a hard error, not ErrNotConfigured", err)
	}
}

func TestLoadSupplyAtRestInvalidPreviousDoesNotEchoValue(t *testing.T) {
	invalid := "not-valid-base64!!"
	_, err := loadSupplyAtRest(supplyKeyInputs{
		currentEnv:  supplyKeyB64(0),
		previousEnv: invalid,
	}, nil)
	if err == nil {
		t.Fatal("err = nil, want an error for an invalid previous key")
	}
	if strings.Contains(err.Error(), invalid) {
		t.Fatalf("error %q echoes the invalid key value", err.Error())
	}
}
