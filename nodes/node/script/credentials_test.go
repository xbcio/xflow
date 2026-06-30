package script

import (
	"errors"
	"strings"
	"testing"
)

func TestResolveCredentials_DeclaredOnly(t *testing.T) {
	resolver := func(name string) (map[string]any, error) {
		switch name {
		case "aes_key":
			return map[string]any{"key": "secret-k", "iv": "iv-v"}, nil
		case "api_token":
			return map[string]any{"token": "t-123"}, nil
		case "unlisted":
			return map[string]any{"token": "should-not-appear"}, nil
		}
		return nil, nil
	}

	creds, first, err := ResolveCredentials([]string{"aes_key", "api_token"}, resolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(creds) != 2 {
		t.Fatalf("creds len = %d, want 2", len(creds))
	}
	if creds["aes_key"].(map[string]any)["key"] != "secret-k" {
		t.Fatalf("aes_key.key = %v", creds["aes_key"])
	}
	if _, leaked := creds["unlisted"]; leaked {
		t.Fatal("gate violated: unlisted credential present in $credentials")
	}
	if first.(map[string]any)["key"] != "secret-k" {
		t.Fatalf("$credential = %v, want aes_key value", first)
	}
}

func TestResolveCredentials_NoDeclaration(t *testing.T) {
	creds, first, err := ResolveCredentials(nil, func(string) (map[string]any, error) { return nil, nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(creds) != 0 {
		t.Fatalf("creds should be empty, got %v", creds)
	}
	if first != nil {
		t.Fatalf("$credential should be nil when nothing declared, got %v", first)
	}
}

func TestResolveCredentials_ResolverError_NoLeak(t *testing.T) {
	resolver := func(name string) (map[string]any, error) {
		return nil, errors.New("credential not found")
	}
	_, _, err := ResolveCredentials([]string{"aes_key"}, resolver)
	if err == nil {
		t.Fatal("expected error when resolver fails")
	}
	if !strings.Contains(err.Error(), "aes_key") {
		t.Fatalf("error %q should name the credential", err.Error())
	}
}

func TestResolveCredentials_NilValue_NotFound(t *testing.T) {
	resolver := func(string) (map[string]any, error) { return nil, nil }
	_, _, err := ResolveCredentials([]string{"missing_cred"}, resolver)
	if err == nil {
		t.Fatal("expected not-found error when resolver returns nil value")
	}
	if !strings.Contains(err.Error(), "missing_cred") {
		t.Fatalf("error %q should name the credential", err.Error())
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error %q should indicate not found", err.Error())
	}
}
