package distributed

import (
	"slices"
	"testing"

	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/store/blob"
)

// blobVocabSorted returns a sorted copy, so two result lists compare as sets.
func blobVocabSorted(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return out
}

// blobVocabCompare reports every way the two lists differ: a result the blob
// policy reports that the metrics adapter would export as "unknown", and a
// result the adapter lists that the policy never reports.
func blobVocabCompare(t *testing.T, kind string, policy, exported []string) {
	t.Helper()
	for _, result := range policy {
		if !slices.Contains(exported, result) {
			t.Errorf("blob policy reports %s result %q but the metrics adapter does not export it (has %v)", kind, result, exported)
		}
	}
	for _, result := range exported {
		if !slices.Contains(policy, result) {
			t.Errorf("metrics adapter exports %s result %q that the blob policy never reports (has %v)", kind, result, policy)
		}
	}
	if !slices.Equal(blobVocabSorted(policy), blobVocabSorted(exported)) {
		t.Errorf("%s results differ: policy %v, metrics %v", kind, policy, exported)
	}
}

// Mutation note: adding a put result to the blob policy without teaching the
// metrics adapter (or the reverse) must turn this red. Otherwise the new result
// would be exported as "unknown" and an alert written for it would never fire.
func TestBlobPutResultVocabularyMatchesMetrics(t *testing.T) {
	blobVocabCompare(t, "put", blob.PutResults(), metrics.BlobPutResults())
}

// Mutation note: adding a get result to the blob policy without teaching the
// metrics adapter (or the reverse) must turn this red, for the same reason as
// the put list above.
func TestBlobGetResultVocabularyMatchesMetrics(t *testing.T) {
	blobVocabCompare(t, "get", blob.GetResults(), metrics.BlobGetResults())
}

// The label value for a result the adapter does not know must not collide with
// one the policy reports, or "unknown" would stop meaning "should stay at zero".
func TestBlobVocabUnknownIsNotAPolicyResult(t *testing.T) {
	for _, result := range append(blob.PutResults(), blob.GetResults()...) {
		if result == metrics.BlobResultUnknown {
			t.Errorf("blob policy reports %q, which the metrics adapter reserves for results it does not know", result)
		}
	}
}

// The adapter lives in a package that cannot import the blob package or this
// one, so it mirrors both observer interfaces with primitive parameters.
// Satisfying them is what lets one value be installed on a blob.Policy and on
// the collector loop; a method that differs in name or signature on either side
// would silently turn that observer off. The two assignments below are the
// compile-time check. Mutation note: renaming OnGC, or changing a parameter type
// on either side, fails the build of this package's tests.
func TestBlobVocabMetricsSatisfiesBothObservers(t *testing.T) {
	adapter := metrics.NewBlobMetrics(metrics.New())

	var policyObserver blob.PolicyObserver = adapter
	var collectorObserver gcObserver = adapter

	policyObserver.OnPut(blob.PutResultOK)
	policyObserver.OnGet(blob.GetResultOK)
	collectorObserver.OnGC(1, 1)
}

// The failure hook is an optional extension of gcObserver, so it is checked on
// its own: losing OnGCFailed, or renaming it or its parameter, would silently
// stop reporting collection failures. Mutation note: any such change fails the
// build of this package's tests.
func TestBlobVocabMetricsSatisfiesFailureObserver(t *testing.T) {
	adapter := metrics.NewBlobMetrics(metrics.New())

	var failureObserver gcFailureObserver = adapter

	failureObserver.OnGCFailed(1)
}
