package main

import (
	"reflect"
	"sort"
	"testing"

	"github.com/xbcio/xflow/node/registry"
)

// builtinNodeTypes is the checked-in list of every builtin node type the
// server process must see. package node keeps no list of its own, so this is
// the reference: adding a builtin means adding it here, and a builtin the
// server binary no longer links fails the test below. That main.go links them
// on purpose (a direct import, not a transitive one) is pinned separately by
// test/architecture's TestServerAssemblyImportsBuiltinNodesDirectly.
var builtinNodeTypes = []string{
	"xflow.approval",
	"xflow.browser.cdp",
	"xflow.database",
	"xflow.end",
	"xflow.function",
	"xflow.grpc",
	"xflow.http",
	"xflow.if",
	"xflow.map",
	"xflow.merge",
	"xflow.notification",
	"xflow.script",
	"xflow.start",
	"xflow.switch",
	"xflow.transform.aggregate",
	"xflow.transform.filter",
	"xflow.transform.limit",
	"xflow.transform.pick",
	"xflow.transform.remove_duplicates",
	"xflow.transform.rename",
	"xflow.transform.set",
	"xflow.transform.sort",
	"xflow.trigger.cron",
	"xflow.trigger.kafka",
	"xflow.trigger.redis",
	"xflow.trigger.timer",
	"xflow.trigger.webhook",
	"xflow.wait",
}

// TestServerProcessRegistersEveryBuiltinType asserts the server binary's
// registry holds exactly the builtin node types -- action and trigger tables
// together, via registry.Descriptors(). Equality rather than a superset: this
// package registers no test handlers, so an extra type is a builtin missing
// from the list above, and a missing one is a builtin the server cannot see.
func TestServerProcessRegistersEveryBuiltinType(t *testing.T) {
	seen := map[string]bool{}
	for _, rd := range registry.Descriptors() {
		seen[rd.Type] = true
	}
	got := make([]string, 0, len(seen))
	for typ := range seen {
		got = append(got, typ)
	}
	sort.Strings(got)

	want := append([]string(nil), builtinNodeTypes...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("server registry types =\n  %v\nwant\n  %v", got, want)
	}
}
