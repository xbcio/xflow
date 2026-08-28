package script

import (
	"reflect"
	"testing"
)

// TestDeclarationsAreRefCounted: the same (workflow, node) pair is declared once
// per activation, and a runner hosts several replicas of the same activation.
// A plain overwrite-then-delete would let replica 1's Deactivate erase replica
// 0's still-live declaration, after which replica 0's next execution finds no
// supplies and -- with §4.3.1's fail-closed -- fails every message.
func TestDeclarationsAreRefCounted(t *testing.T) {
	tbl := newSupplyDeclarationTable()
	tbl.declare("collect", "decode", []string{"rules"})
	tbl.declare("collect", "decode", []string{"rules"})

	tbl.undeclare("collect", "decode", []string{"rules"})
	if got := tbl.lookup("collect", "decode"); !reflect.DeepEqual(got, []string{"rules"}) {
		t.Fatalf("after one undeclare of two declares, lookup = %v, want [rules]", got)
	}

	tbl.undeclare("collect", "decode", []string{"rules"})
	if got := tbl.lookup("collect", "decode"); got != nil {
		t.Fatalf("after the second undeclare, lookup = %v, want nil", got)
	}
}

// TestLookupIsSortedAndCopied: callers iterate the result and must not be able
// to mutate the table through it, and two runs must produce the same order so an
// error message naming "the first supply" is stable.
func TestLookupIsSortedAndCopied(t *testing.T) {
	tbl := newSupplyDeclarationTable()
	tbl.declare("collect", "decode", []string{"zeta", "alpha"})

	got := tbl.lookup("collect", "decode")
	if !reflect.DeepEqual(got, []string{"alpha", "zeta"}) {
		t.Fatalf("lookup = %v, want sorted [alpha zeta]", got)
	}
	got[0] = "mutated"
	if again := tbl.lookup("collect", "decode"); again[0] != "alpha" {
		t.Fatalf("the table was mutated through a returned slice: %v", again)
	}
}

// TestDistinctNodesDoNotShare: SAS runs decode and clean under the same map body
// name. Keying by workflow alone would give decode clean's supplies, which is
// exactly the failure mode spec §4.2.1 candidate 3 describes -- and which SAS's
// current topology (both nodes read the same supply) would hide.
func TestDistinctNodesDoNotShare(t *testing.T) {
	tbl := newSupplyDeclarationTable()
	tbl.declare("collect", "decode", []string{"decode-rules"})
	tbl.declare("collect", "clean", []string{"clean-rules"})

	if got := tbl.lookup("collect", "decode"); !reflect.DeepEqual(got, []string{"decode-rules"}) {
		t.Fatalf("decode sees %v, want [decode-rules]", got)
	}
	if got := tbl.lookup("collect", "clean"); !reflect.DeepEqual(got, []string{"clean-rules"}) {
		t.Fatalf("clean sees %v, want [clean-rules]", got)
	}
}

// TestUnknownNodeReturnsNil: a node with no declaration must read as "nothing
// declared", not as an empty-but-present entry. §4.3.1's guard only engages when
// there IS a declaration, so a phantom entry would fail-close a plain inline
// wasm node that never had a supply.
func TestUnknownNodeReturnsNil(t *testing.T) {
	tbl := newSupplyDeclarationTable()
	if got := tbl.lookup("nobody", "nothing"); got != nil {
		t.Fatalf("lookup on an unknown pair = %v, want nil", got)
	}
}
