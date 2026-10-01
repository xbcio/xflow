package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"
)

func TestRunSupplyResealRequiresMySQLDSN(t *testing.T) {
	var out bytes.Buffer
	opts := &supplyResealOptions{out: &out}
	err := runSupplyReseal(context.Background(), opts)
	if err == nil {
		t.Fatal("err = nil, want an error when --mysql-dsn is not set")
	}
	if !strings.Contains(err.Error(), "mysql-dsn") {
		t.Fatalf("err = %v, want it to mention --mysql-dsn", err)
	}
}

func TestRunSupplyResealRequiresMasterKey(t *testing.T) {
	t.Setenv("XFLOW_MASTER_KEY", "")
	t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", "")
	var out bytes.Buffer
	opts := &supplyResealOptions{out: &out, mysqlDSN: "root:x@tcp(127.0.0.1:1)/xflow?parseTime=true"}
	err := runSupplyReseal(context.Background(), opts)
	if err == nil {
		t.Fatal("err = nil, want an error when no current master key is configured")
	}
	if !strings.Contains(err.Error(), "master key") && !strings.Contains(err.Error(), "XFLOW_MASTER_KEY") {
		t.Fatalf("err = %v, want it to explain a current key is required", err)
	}
}

func TestBuildResealAtRestPreviousEqualsCurrentIsError(t *testing.T) {
	same := supplyKeyB64ForCLI(0)
	t.Setenv("XFLOW_MASTER_KEY", same)
	t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", same)
	opts := &supplyResealOptions{}
	_, err := buildResealAtRest(opts)
	if err == nil {
		t.Fatal("err = nil, want an error when previous equals current")
	}
}

func TestBuildResealAtRestCurrentOnly(t *testing.T) {
	t.Setenv("XFLOW_MASTER_KEY", supplyKeyB64ForCLI(0))
	t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", "")
	opts := &supplyResealOptions{}
	a, err := buildResealAtRest(opts)
	if err != nil {
		t.Fatalf("buildResealAtRest: %v", err)
	}
	if a == nil {
		t.Fatal("a = nil, want a configured AtRest")
	}
}

func TestBuildResealAtRestInvalidPreviousDoesNotEchoValue(t *testing.T) {
	invalid := "not-valid-base64!!"
	t.Setenv("XFLOW_MASTER_KEY", supplyKeyB64ForCLI(0))
	t.Setenv("XFLOW_MASTER_KEY_PREVIOUS", invalid)
	opts := &supplyResealOptions{}
	_, err := buildResealAtRest(opts)
	if err == nil {
		t.Fatal("err = nil, want an error for an invalid previous key")
	}
	if strings.Contains(err.Error(), invalid) {
		t.Fatalf("error %q echoes the invalid key value", err.Error())
	}
}

func TestSupplyResealCommandRegistered(t *testing.T) {
	var out bytes.Buffer
	root := newRootCommand(&out)
	cmd, _, err := root.Find([]string{"supply", "reseal"})
	if err != nil {
		t.Fatalf("Find(supply reseal): %v", err)
	}
	if cmd == nil || cmd.Use != "reseal" {
		t.Fatalf("cmd = %+v, want the reseal subcommand", cmd)
	}
}

// supplyKeyB64ForCLI builds a synthetic base64-encoded 32-byte key for tests
// in this package, mirroring cmd/server's fixture helper of the same shape
// (unexported there, so it cannot be shared across the two main packages).
func supplyKeyB64ForCLI(fill byte) string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = fill + byte(i)
	}
	return base64.StdEncoding.EncodeToString(raw)
}
