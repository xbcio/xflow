package wasm

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// taggerConfig builds a $config globals entry for the tagger guest.
func taggerConfig(rules ...map[string]any) map[string]any {
	rs := make([]any, 0, len(rules))
	for _, r := range rules {
		rs = append(rs, r)
	}
	return map[string]any{"rules": rs}
}

func cleanRule(field, when string) map[string]any {
	return map[string]any{"kind": "clean", "field": field, "when": when}
}

func tagRule(tag, when string) map[string]any {
	return map[string]any{"kind": "tag", "tag": tag, "when": when}
}

// taggerResult splits a tagger eval result into its cleansed record and tag set.
func taggerResult(t *testing.T, out any) (map[string]any, map[string]bool) {
	t.Helper()
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result not an object: %#v", out)
	}
	rec, _ := m["record"].(map[string]any)
	tags := map[string]bool{}
	arr, _ := m["tags"].([]any)
	for _, v := range arr {
		tags[v.(string)] = true
	}
	return rec, tags
}

// TestTagger_CleansAndTags is the core contract: a conditional tag fires, an
// unconditional clean strips a field, and both happen in one eval.
func TestTagger_CleansAndTags(t *testing.T) {
	e := newReactor(t)
	out, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config": taggerConfig(
			cleanRule("authorization", ""),
			tagRule("admin-api", `path startsWith "/admin"`),
			tagRule("slow", "latency_ms > 1000"),
		),
		"path":          "/admin/users",
		"latency_ms":    120.0,
		"authorization": "Bearer secret-value",
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	rec, tags := taggerResult(t, out)

	if _, present := rec["authorization"]; present {
		t.Fatalf("authorization survived the clean rule: %v", rec)
	}
	if rec["path"] != "/admin/users" {
		t.Fatalf("path = %v, want /admin/users", rec["path"])
	}
	if !tags["admin-api"] {
		t.Fatalf("admin-api tag missing: %v", tags)
	}
	if tags["slow"] {
		t.Fatalf("slow tag fired at latency_ms=120: %v", tags)
	}
}

// TestTagger_CleanedFieldDoesNotSurviveUnderEnvRoots is the leak regression.
//
// The host hands the guest its whole expr environment as the eval input (see
// reactor.go Execute: everything but $config), and that environment carries the
// node's data TWICE — merged at the top level AND again under the $input /
// $inputs roots (exprx.BuildExprEnv). A guest that decodes its eval input and
// emits it as "the record" therefore ships a second, uncleansed copy of every
// field a clean rule just stripped. For the SAS pipeline that means the
// credential the rules exist to remove still reaches the persisted output.
//
// The assertion is a recursive scan rather than a top-level key check, because
// a fix that merely relocated the roots would pass a shallow one.
func TestTagger_CleanedFieldDoesNotSurviveUnderEnvRoots(t *testing.T) {
	e := newReactor(t)
	// The record as the engine env actually presents it: top-level merged, and
	// the identical map under the $input/$inputs roots.
	record := map[string]any{
		"path":          "/admin/users",
		"latency_ms":    42.0,
		"authorization": "Bearer super-secret",
	}
	out, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config":       taggerConfig(cleanRule("authorization", "")),
		"path":          record["path"],
		"latency_ms":    record["latency_ms"],
		"authorization": record["authorization"],
		"$input":        record,
		"$inputs":       map[string]any{"main": record},
		"$params":       map[string]any{"language": "wasm", "runtime": "wazero-reactor"},
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	rec, _ := taggerResult(t, out)

	if found := findValue(rec, "Bearer super-secret"); found != "" {
		t.Fatalf("cleaned credential survives in the output record at %s: %v", found, rec)
	}
	// Guard against passing by having emptied the record: the fields the rules
	// did NOT strip must still be there.
	if rec["path"] != "/admin/users" {
		t.Fatalf("path = %v, want /admin/users", rec["path"])
	}
	if rec["latency_ms"] != 42.0 {
		t.Fatalf("latency_ms = %v, want 42", rec["latency_ms"])
	}
}

// findValue walks v recursively and returns the path at which want appears as a
// string value, or "" if it does not appear at all.
func findValue(v any, want string) string {
	switch t := v.(type) {
	case string:
		if t == want {
			return "."
		}
	case map[string]any:
		for k, e := range t {
			if p := findValue(e, want); p != "" {
				return "." + k + strings.TrimPrefix(p, ".")
			}
		}
	case []any:
		for i, e := range t {
			if p := findValue(e, want); p != "" {
				return fmt.Sprintf("[%d]%s", i, strings.TrimPrefix(p, "."))
			}
		}
	}
	return ""
}

// TestTagger_CleanDoesNotHideFieldFromTagRules pins the ordering guarantee: tag
// rules see the ORIGINAL record, so a clean rule listed first cannot change
// whether a later tag rule matches. Without this the result would silently
// depend on rule order in a way the rule author cannot see.
func TestTagger_CleanDoesNotHideFieldFromTagRules(t *testing.T) {
	e := newReactor(t)
	out, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config": taggerConfig(
			cleanRule("token", ""),
			tagRule("had-token", `token != nil`),
		),
		"token": "t-abc",
		"path":  "/v1/x",
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	rec, tags := taggerResult(t, out)

	if _, present := rec["token"]; present {
		t.Fatalf("token survived the clean rule: %v", rec)
	}
	if !tags["had-token"] {
		t.Fatalf("tag rule did not see the pre-clean record: %v", tags)
	}
}

// TestTagger_ConditionalClean covers a clean rule that only strips when its
// condition holds — the same field survives when it does not.
func TestTagger_ConditionalClean(t *testing.T) {
	e := newReactor(t)
	cfg := taggerConfig(cleanRule("body", `path startsWith "/internal"`))

	stripped, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config": cfg,
		"path":    "/internal/dump",
		"body":    "sensitive",
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("eval (match): %v", err)
	}
	rec, _ := taggerResult(t, stripped)
	if _, present := rec["body"]; present {
		t.Fatalf("body survived a matching clean rule: %v", rec)
	}

	kept, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config": cfg,
		"path":    "/public/info",
		"body":    "harmless",
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("eval (no match): %v", err)
	}
	rec2, _ := taggerResult(t, kept)
	if rec2["body"] != "harmless" {
		t.Fatalf("body = %v, want harmless (clean rule should not have fired)", rec2["body"])
	}
}

// TestTagger_BadRuleRejected proves a malformed rule set is rejected whole
// rather than partially applied — the host keeps its last-good pool (docs §6.3).
func TestTagger_BadRuleRejected(t *testing.T) {
	e := newReactor(t)
	_, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config": taggerConfig(map[string]any{"kind": "nonsense", "tag": "x"}),
		"path":    "/v1/x",
	}, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected an unknown rule kind to be rejected")
	}
}

// TestTagger_EmptyRuleSetIsValid pins that "no rules" is a legitimate config
// (docs §6: an empty rule set means pass-through, not an error).
func TestTagger_EmptyRuleSetIsValid(t *testing.T) {
	e := newReactor(t)
	out, err := e.Execute(context.Background(), b64(taggerWasm), map[string]any{
		"$config": taggerConfig(),
		"path":    "/v1/x",
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("empty rule set rejected: %v", err)
	}
	rec, tags := taggerResult(t, out)
	if rec["path"] != "/v1/x" {
		t.Fatalf("record altered by an empty rule set: %v", rec)
	}
	if len(tags) != 0 {
		t.Fatalf("tags produced by an empty rule set: %v", tags)
	}
}
