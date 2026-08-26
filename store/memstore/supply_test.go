package memstore_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/storetest"
)

func TestMemstoreSupplyContract(t *testing.T) {
	storetest.SupplyContract(t, memstore.New(), "mem")
}

func TestMemstoreSupplyNamespaceNorm(t *testing.T) {
	storetest.SupplyNamespaceNormContract(t, memstore.New())
}

// TestMemstoreGetSupply_ContentIsolatedFromCaller pins GetSupply's defensive
// copy of Content (store/memstore/supply.go: `cp.Content = append([]byte(nil),
// rec.Content...)`). Every existing assertion on GetSupply's Content only
// compares it by value (string(rec.Content) == ...), which passes whether the
// returned slice is a fresh copy or an alias into the store's internal
// backing array. Deleting that copy line and returning `&cp` with `cp.Content`
// still sharing rec's backing array compiles and leaves storetest.SupplyContract
// and TestMemstoreSupplyNamespaceNorm green, because neither ever mutates a
// slice it got back from GetSupply. A caller that does mutate its copy in
// place would otherwise corrupt the stored bytes for every subsequent reader.
func TestMemstoreGetSupply_ContentIsolatedFromCaller(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	const ns, name = "iso-ns", "iso-supply"

	original := []byte(`{"v":1}`)
	if _, err := s.PutSupply(ctx, &store.SupplyResource{
		Namespace: ns, Name: name, Content: original,
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	got, err := s.GetSupply(ctx, ns, name)
	if err != nil {
		t.Fatalf("GetSupply: %v", err)
	}
	// Mutate the caller's copy in place.
	for i := range got.Content {
		got.Content[i] = 'X'
	}

	fresh, err := s.GetSupply(ctx, ns, name)
	if err != nil {
		t.Fatalf("GetSupply after mutation: %v", err)
	}
	if string(fresh.Content) != `{"v":1}` {
		t.Fatalf("stored content = %q after caller mutated its returned slice, want %q: "+
			"GetSupply must return a copy, not an alias into the store's backing array",
			fresh.Content, `{"v":1}`)
	}
}
