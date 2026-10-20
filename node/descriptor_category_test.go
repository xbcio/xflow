package node_test

// Contract tests over the two editor node-card fields every builtin
// Descriptor declares: Category (spec §5) and CardSummary (spec §6.1.1).
// The editor draws a node's icon, colour and card rows from them, so a typo,
// a category outside the table, or a summary naming a param that does not
// exist has to fail here rather than in the browser.

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// categoryByTypePrefix restates the spec §5 table as the category a builtin
// type must declare. It is the same prefix mapping the editor's fallback
// (xflow-core categoryMeta.ts categoryOf) applies when a descriptor declares
// nothing, so a builtin that disagrees with this table would change category
// the moment the descriptor started declaring one.
var categoryByTypePrefix = []struct {
	prefix   string
	category string
}{
	{"xflow.trigger", "trigger"},
	{"xflow.transform", "transform"},
	{"xflow.approval", "human"},
	{"xflow.if", "control"},
	{"xflow.switch", "control"},
	{"xflow.merge", "control"},
	{"xflow.map", "control"},
	{"xflow.wait", "control"},
	{"xflow.start", "control"},
	{"xflow.end", "control"},
	{"xflow.subworkflow", "control"},
	{"xflow.function", "code"},
	{"xflow.script", "code"},
	{"xflow.http", "action"},
	{"xflow.grpc", "action"},
	{"xflow.database", "action"},
	{"xflow.notification", "action"},
	{"xflow.browser", "action"},
	{"xflow.supply", "unknown"},
}

// nodeCategories is the closed enum from spec §5, restated on the server side.
var nodeCategories = map[string]bool{
	"trigger": true, "control": true, "action": true, "code": true,
	"transform": true, "human": true, "unknown": true,
}

func TestBuiltinDescriptorsDeclareCategoryAndCardSummary(t *testing.T) {
	descs := builtinDescriptors(t)
	if len(descs) < 32 {
		t.Fatalf("only %d builtin descriptors found, want at least 32", len(descs))
	}
	for _, d := range descs {
		desc := d.desc
		t.Run(desc.Type, func(t *testing.T) {
			if !nodeCategories[desc.Category] {
				t.Errorf("category %q is not in spec §5", desc.Category)
			}
			want := ""
			for _, p := range categoryByTypePrefix {
				if desc.Type == p.prefix || strings.HasPrefix(desc.Type, p.prefix+".") {
					want = p.category
					break
				}
			}
			if want == "" {
				t.Fatalf("no spec §5 category for %s", desc.Type)
			}
			if desc.Category != want {
				t.Errorf("category = %q, want %q", desc.Category, want)
			}

			topLevel := make(map[string]bool, len(desc.Params))
			for _, p := range desc.Params {
				topLevel[p.Name] = true
			}
			// A node with no params has nothing to summarise; start and end
			// declare no summary rather than an empty one.
			if len(desc.Params) == 0 {
				if len(desc.CardSummary) != 0 {
					t.Errorf("paramless node declares card_summary %v", desc.CardSummary)
				}
				return
			}
			if n := len(desc.CardSummary); n == 0 || n > 3 {
				t.Errorf("card_summary has %d rows, want 1..3: %v", n, desc.CardSummary)
			}
			seen := map[string]bool{}
			for _, row := range desc.CardSummary {
				if row == "" {
					t.Errorf("card_summary contains an empty row")
					continue
				}
				if seen[row] {
					t.Errorf("card_summary repeats %q", row)
				}
				seen[row] = true
				parts := strings.Split(row, "+")
				if len(parts) > 2 {
					t.Errorf("card_summary %q joins %d params, want at most 2", row, len(parts))
				}
				used := map[string]bool{}
				for _, part := range parts {
					if !topLevel[part] {
						t.Errorf("card_summary %q names %q, not a top-level param of %s", row, part, desc.Type)
					}
					if used[part] {
						t.Errorf("card_summary %q names %q twice", row, part)
					}
					used[part] = true
				}
			}
		})
	}
}

// TestBuiltinCardSummaryMatchesPreviewBaseline pins the rows for the kinds the
// /ui-preview cardprops gallery is the visual source of truth for: which params
// go on the card, in order, and the `a+b` join for compound rows. A gallery
// row that names no real param (HTTP's retry, notification's secret) is not
// declared: card_summary carries param names only.
func TestBuiltinCardSummaryMatchesPreviewBaseline(t *testing.T) {
	want := map[string][]string{
		"xflow.trigger.webhook": {"method+path", "secret_supply"},
		"xflow.trigger.cron":    {"expression", "timezone"},
		"xflow.http":            {"method+url", "authentication"},
		"xflow.database":        {"operation+table", "credential", "where"},
		"xflow.if":              {"condition"},
		"xflow.switch":          {"rules", "expression"},
		"xflow.approval":        {"approvers", "mode+timeout"},
		"xflow.notification":    {"channel", "to"},
	}
	byType := map[string]types.Descriptor{}
	for _, d := range builtinDescriptors(t) {
		byType[d.desc.Type] = d.desc
	}
	for typ, rows := range want {
		desc, ok := byType[typ]
		if !ok {
			t.Fatalf("%s is not a builtin descriptor", typ)
		}
		if strings.Join(desc.CardSummary, "|") != strings.Join(rows, "|") {
			t.Errorf("%s card_summary = %v, want %v", typ, desc.CardSummary, rows)
		}
	}
}
