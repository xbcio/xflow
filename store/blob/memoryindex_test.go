package blob_test

import (
	"testing"

	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/blob/blobtest"
)

func memoryIndexFactory(t *testing.T) blobtest.IndexHarness {
	clock := blobtest.NewClock()
	return blobtest.IndexHarness{Index: blob.NewMemoryIndex(clock.Now), Clock: clock}
}

func TestMemoryIndexSatisfiesContract(t *testing.T) {
	blobtest.RunIndexContract(t, memoryIndexFactory)
}
