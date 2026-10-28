// Package blobtest holds the behavioural contract every blob.Store must pass.
//
// It is a normal package rather than a _test file so that each implementation's
// own test file can import it: the in-memory store, the file store, and the
// objectstore-backed store all run the same cases through RunStoreContract.
package blobtest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
)

// Factory returns a brand-new, empty Store on every call.
type Factory func(t *testing.T) blob.Store

// Namespaces used by the contract cases. They are valid blob namespaces.
const (
	nsA namespace.Namespace = "contract-a"
	nsB namespace.Namespace = "contract-b"
)

// RunStoreContract runs the blob.Store contract against stores built by
// newStore. Each case gets its own fresh Store, so cases never depend on one
// another and can be run alone with -run.
func RunStoreContract(t *testing.T, newStore Factory) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, s blob.Store)
	}{
		{"PutGetRoundTrip", caseRoundTrip},
		{"PutIsIdempotent", caseIdempotent},
		{"DifferentContentDifferentDigest", caseDifferentDigest},
		{"RefFields", caseRefFields},
		{"NamespaceIsolation", caseNamespaceIsolation},
		{"PutRejectsInvalidNamespace", casePutRejectsInvalidNamespace},
		{"InvalidRefNamespaceRejected", caseInvalidRefNamespace},
		{"GetMissing", caseGetMissing},
		{"GetRejectsInvalidRef", caseGetRejectsInvalidRef},
		{"GetDetectsSizeMismatch", caseGetDetectsSizeMismatch},
		{"DeleteThenGet", caseDeleteThenGet},
		{"DeleteMissingIsNil", caseDeleteMissing},
		{"DeleteKeepsOtherBlobs", caseDeleteKeepsOthers},
		{"DeleteChecksNamespace", caseDeleteChecksNamespace},
		{"DeleteIsPerNamespace", caseDeleteIsPerNamespace},
		{"PutDoesNotAliasInput", caseNoAliasing},
		{"ContextCancelled", caseContextCancelled},
		{"ConcurrentPutSameContent", caseConcurrentPut},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newStore(t))
		})
	}
}

// sha256Hex returns the lowercase hex SHA-256 of data, computed independently
// of the Store under test.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// randomBytes returns n cryptographically random bytes.
func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read(%d) error = %v", n, err)
	}
	return b
}
