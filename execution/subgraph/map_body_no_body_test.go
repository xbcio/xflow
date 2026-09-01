package subgraph

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestExecuteBatchBody_NoBodyIsPermanent pins the classification of a batch whose
// request carries no projected body package.
//
// The two assertions are deliberately different in kind, and only the second is
// load-bearing:
//
//	identity     -- errors.Is(err, ErrNoMapBody) makes the name usable. Eleven
//	                comments and test strings already referred to this failure as
//	                `ErrNoMapBody` while no such symbol existed, so nothing could
//	                match on it.
//	permanence   -- types.IsPermanent(err) is what changes behaviour. A nil Body
//	                is a compile-time projection outcome: the identical request
//	                redelivered finds the identical nil. Unmarked, it is treated
//	                as transient by every retry-capable queue in this repo, which
//	                is how it surfaced as a downstream batch deadline rather than
//	                as the configuration error it is.
//
// Both are asserted with t.Errorf rather than t.Fatalf so neither failure hides
// the other: reverting the sentinel to a bare errors.New must show up as two
// distinct complaints, not one.
func TestExecuteBatchBody_NoBodyIsPermanent(t *testing.T) {
	x := &MapBodyExecutor{}

	_, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:  nil,
		Items: []any{"one", "two"},
	})
	if err == nil {
		t.Fatal("a batch request with a nil Body returned no error; the batch would " +
			"run zero items and report success, silently dropping every item in it")
	}

	if !errors.Is(err, ErrNoMapBody) {
		t.Errorf("errors.Is(err, ErrNoMapBody) is false for %v. Callers that want to "+
			"distinguish a missing body from any other batch failure have nothing to "+
			"match on, which is the state eleven comments already assumed was fixed.", err)
	}

	if !types.IsPermanent(err) {
		t.Errorf("types.IsPermanent(err) is false for %v. A nil Body cannot be repaired "+
			"by redelivering the same request, but an unmarked error is retried as "+
			"transient -- the failure then reports as a deadline downstream instead of "+
			"as the permanent configuration error it is.", err)
	}

	// The code is the operator-facing grep anchor, so it is asserted as a literal
	// rather than as a reference to the constant that produces it -- a test that
	// reads the value back from the symbol under test cannot notice it changing.
	if !strings.Contains(err.Error(), "no_map_body") {
		t.Errorf("error text %q does not contain the stable code \"no_map_body\"", err.Error())
	}
}

// TestExecuteBatchBody_NoBodyBeatsEmptyItems pins the ORDER of the two early
// returns: a request that is missing its body AND has no items must report the
// missing body, not success.
//
// The empty-items branch returns an empty result set and a nil error. It sits
// directly below the body check, so swapping the two would convert a permanent
// configuration fault into a silent success on exactly the batches that carry no
// items -- the shape a drained partition produces.
func TestExecuteBatchBody_NoBodyBeatsEmptyItems(t *testing.T) {
	x := &MapBodyExecutor{}

	_, err := x.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:  nil,
		Items: nil,
	})
	if !errors.Is(err, ErrNoMapBody) {
		t.Errorf("a bodiless batch with no items returned %v, want ErrNoMapBody: the "+
			"missing body is a permanent fault and must not be masked by the "+
			"empty-items fast path", err)
	}
}
