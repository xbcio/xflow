package metrics

// Blob store metric names. They are registered lazily, on the first observation,
// so a deployment that does not enable blob externalization never creates the
// series: nothing calls the adapter below unless blob storage was selected and
// wired to it, and a collector pass that deleted nothing reports nothing.
const (
	// metricBlobPut counts payloads the blob policy tried to store, partitioned
	// by a bounded result. A payload small enough to stay inline never reaches it.
	metricBlobPut = "xflow_blob_put_total"
	// metricBlobGet counts blob reads, partitioned by a bounded result.
	metricBlobGet = "xflow_blob_get_total"
	// metricBlobGCDeleted counts blobs the collector deleted.
	metricBlobGCDeleted = "xflow_blob_gc_deleted_total"
	// metricBlobGCSkippedGeneration counts blobs the collector found unreferenced
	// but left alone because a newer write fenced the deletion.
	metricBlobGCSkippedGeneration = "xflow_blob_gc_skipped_generation_total"
	// metricBlobGCFailed counts collection failures: blobs the collector could not
	// delete, plus one for a pass that failed as a whole.
	metricBlobGCFailed = "xflow_blob_gc_failed_total"
)

// Blob put results. The set is closed: BlobMetrics maps anything else to
// BlobResultUnknown, so a store that gains a result before this table does
// cannot widen the label's cardinality. They mirror the vocabulary of the blob
// policy (store/blob), which this package must not import; a test in the
// distributed package keeps the lists equal.
const (
	blobPutOK       = "ok"
	blobPutTooLarge = "too_large"
	blobPutError    = "error"
)

// Blob get results; see the put results above for why the set is closed and how
// it is kept equal to the policy's.
const (
	blobGetOK        = "ok"
	blobGetMissing   = "missing"
	blobGetCorrupt   = "corrupt"
	blobGetNamespace = "namespace"
	blobGetError     = "error"
)

// BlobResultUnknown is the label value both blob counters use for a result this
// adapter does not know. It should stay at zero.
const BlobResultUnknown = "unknown"

// blobPutResults is every put result this adapter recognises.
var blobPutResults = map[string]struct{}{
	blobPutOK:       {},
	blobPutTooLarge: {},
	blobPutError:    {},
}

// blobGetResults is every get result this adapter recognises.
var blobGetResults = map[string]struct{}{
	blobGetOK:        {},
	blobGetMissing:   {},
	blobGetCorrupt:   {},
	blobGetNamespace: {},
	blobGetError:     {},
}

// BlobPutResults returns the put results this adapter recognises, which the blob
// policy reports. A result outside the list is exported as BlobResultUnknown. It
// exists so the distributed package can check the two vocabularies against each
// other.
func BlobPutResults() []string {
	return []string{blobPutOK, blobPutTooLarge, blobPutError}
}

// BlobGetResults returns the get results this adapter recognises, which the blob
// policy reports. A result outside the list is exported as BlobResultUnknown. It
// exists so the distributed package can check the two vocabularies against each
// other.
func BlobGetResults() []string {
	return []string{blobGetOK, blobGetMissing, blobGetCorrupt, blobGetNamespace, blobGetError}
}

// blobObserver mirrors the two observers blob traffic is reported through: the
// blob policy's (OnPut, OnGet) and the collector loop's (OnGC). It is declared
// with primitive parameters so this package imports neither the blob package nor
// backend providers; Go's structural typing lets one adapter serve both.
type blobObserver interface {
	OnPut(result string)
	OnGet(result string)
	OnGC(deleted, fenced int)
}

// BlobMetrics observes blob externalization and collection. Install it only when
// blob storage is selected; the default deployment must not register it.
//
// Nothing here carries a namespace label: the collector sweeps every namespace in
// one pass, so a per-namespace split of its totals does not exist, and the
// request counters would multiply their cardinality by the namespace count for
// no operational gain. A collector pass that was skipped is logged but not
// counted; a pass that failed is logged and counted, as are the objects it could
// not delete.
type BlobMetrics struct {
	Metrics *Metrics
}

// NewBlobMetrics creates a blob observer backed by Metrics.
func NewBlobMetrics(metrics *Metrics) BlobMetrics {
	return BlobMetrics{Metrics: metrics}
}

// OnPut counts one attempt to store a payload under its result. A result outside
// the closed set is counted as BlobResultUnknown rather than minting a series.
func (b BlobMetrics) OnPut(result string) {
	if _, ok := blobPutResults[result]; !ok {
		result = BlobResultUnknown
	}
	b.Metrics.Inc(metricBlobPut, map[string]string{"result": result})
}

// OnGet counts one blob read under its result. A result outside the closed set
// is counted as BlobResultUnknown rather than minting a series.
func (b BlobMetrics) OnGet(result string) {
	if _, ok := blobGetResults[result]; !ok {
		result = BlobResultUnknown
	}
	b.Metrics.Inc(metricBlobGet, map[string]string{"result": result})
}

// OnGC adds the outcome of one collector pass: blobs deleted, and blobs left in
// place because a newer write fenced the deletion. A zero adds nothing, so a
// pass that found no garbage never creates either series.
func (b BlobMetrics) OnGC(deleted, fenced int) {
	if deleted > 0 {
		b.Metrics.Add(metricBlobGCDeleted, nil, float64(deleted))
	}
	if fenced > 0 {
		b.Metrics.Add(metricBlobGCSkippedGeneration, nil, float64(fenced))
	}
}

// OnGCFailed adds the failure count of one collector pass: blobs it could not
// delete, plus one for a pass that failed as a whole. A zero or negative value
// adds nothing, so a pass with no failure never creates the series.
func (b BlobMetrics) OnGCFailed(failed int) {
	if failed > 0 {
		b.Metrics.Add(metricBlobGCFailed, nil, float64(failed))
	}
}

var _ blobObserver = BlobMetrics{}
