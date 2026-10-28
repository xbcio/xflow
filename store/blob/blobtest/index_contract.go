package blobtest

import (
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
)

// Clock is the controllable time source an Index under test must read. The
// factory builds the Index on Clock.Now and hands the Clock back through
// IndexHarness so cases can cross grace periods and link expiries without
// waiting.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock returns a Clock set to a fixed instant. The instant is whole
// milliseconds, the precision every Index stores.
func NewClock() *Clock {
	return &Clock{now: time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)}
}

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// IndexHarness is a brand-new, empty Index together with the clock it reads.
type IndexHarness struct {
	Index blob.Index
	Clock *Clock
}

// IndexFactory returns a new IndexHarness on every call. An implementation that
// shares storage between calls (a SQL database) must hand back an Index that
// sees none of the previous harness's objects or links.
type IndexFactory func(t *testing.T) IndexHarness

// Constants shared by the Index cases. grace and the horizon are far apart so a
// case can sit between them.
const (
	grace   = time.Hour
	horizon = 24 * time.Hour
	batch   = 100
)

// RunIndexContract runs the blob.Index contract against indexes built by
// newIndex. Each case gets its own fresh harness, so cases never depend on one
// another and can be run alone with -run.
func RunIndexContract(t *testing.T, newIndex IndexFactory) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, h IndexHarness)
	}{
		{"RegisterCreatesLiveObject", caseRegisterCreates},
		{"RegisterReadsBackRecordedMetadata", caseRegisterReadsBack},
		{"RegisterRejectsSizeConflict", caseRegisterSizeConflict},
		{"RegisterRejectsInvalidRef", caseRegisterInvalid},
		{"RegisterConcurrentSameObject", caseRegisterConcurrent},
		{"NamespacesAreDistinctAndCaseSensitive", caseIndexNamespaces},
		{"LinkNeedsRegisteredObject", caseLinkUnregistered},
		{"LinkRejectsBadArguments", caseLinkBadArguments},
		{"LinkOnlyMovesExpiryLater", caseLinkMovesLater},
		{"CandidateNeedsGraceAndNoLink", caseCandidateRules},
		{"ManyExecutionsEachProtect", caseManyLinks},
		{"LastRefNeverMovesBack", caseLastRefNeverMovesBack},
		{"CandidatesOrderAndLimit", caseCandidatesOrder},
		{"PruneLinksRemovesOnlyExpired", casePruneLinks},
		{"MarkDeletingRechecksEligibility", caseMarkRechecks},
		{"MarkDeletingIsIdempotent", caseMarkIdempotent},
		{"LinkIsFencedOnClaimedObject", caseLinkFenced},
		{"PurgeRemovesObjectAndLinks", casePurgeRemoves},
		{"RegisterRevivesClaimedObject", caseReviveBumpsGeneration},
		{"PurgeFailsAfterRevive", casePurgeFencedByRegister},
		{"StaleFenceNeverPurgesALaterClaim", caseStaleFence},
		{"PurgeFailureKeepsClaim", casePurgeDeleteFails},
		{"PurgeRejectsMalformedFence", casePurgeBadFence},
		{"PurgeTwiceIsFenced", casePurgeTwice},
		{"PurgeHoldsOffRegister", casePurgeExcludesRegister},
		{"ContextCancelled", caseIndexContextCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newIndex(t))
		})
	}
}

// refFor returns the Ref a raw (uncompressed) writer would produce for content
// in ns. The digest is computed here, independently of the Index.
func refFor(ns namespace.Namespace, content string) blob.Ref {
	return blob.Ref{
		Namespace: string(ns),
		Digest:    sha256Hex([]byte(content)),
		Size:      int64(len(content)),
		Stored:    int64(len(content)),
	}
}
