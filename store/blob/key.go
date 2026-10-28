package blob

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/xbcio/xflow/namespace"
)

// validateNamespace reports whether ns is safe to use as a blob namespace. The
// object key hex-encodes the namespace (see objectKey), so path traversal can
// no longer come through it; this check stays as defence in depth and keeps
// namespaces that would look hostile in logs or error messages out of the system.
//
// namespace.Validate only keeps Redis key delimiters out. It lets '/' and ".."
// through, so on top of it this rejects any '/', any ".." substring, the bare
// segment ".", whitespace, and control characters. It does not reject upper
// case: real namespaces may contain it, and objectKey keeps them distinct.
func validateNamespace(ns namespace.Namespace) error {
	if err := namespace.Validate(ns); err != nil {
		return err
	}
	s := string(ns)
	switch {
	case s == ".":
		return errors.New("namespace must not be \".\"")
	case strings.Contains(s, ".."):
		return errors.New("namespace must not contain \"..\"")
	case strings.Contains(s, "/"):
		return errors.New("namespace must not contain '/'")
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("namespace must not contain whitespace or control character %U", r)
		}
	}
	return nil
}

// checkNamespace is validateNamespace with its failure wrapped in ErrInvalidRef,
// the form every Store method returns for a namespace it rejects.
func checkNamespace(ns namespace.Namespace) error {
	if err := validateNamespace(ns); err != nil {
		return fmt.Errorf("%w: namespace %q: %w", ErrInvalidRef, string(ns), err)
	}
	return nil
}

// objectKey returns the storage key of the blob with the given digest in ns:
//
//	blobs/<nsseg>/sha256/<hex[0:2]>/<hex[2:4]>/<hex>
//
// nsseg is the lowercase hex encoding of the namespace bytes, not the namespace
// itself. The key becomes a file path in the local backend, and a
// case-insensitive filesystem (default APFS, NTFS) resolves "Tenant" and
// "tenant" to one directory, as does a normalizing one for NFC and NFD forms of
// the same text. Two different namespaces would then share blobs. Hex is
// injective and uses only [0-9a-f], so no two namespaces can map to the same
// path however the filesystem folds names. A namespace is at most
// namespace.MaxNameLen bytes, so the segment is at most 128 characters, below
// the 255 byte limit of a path component.
//
// The two-level fan-out keeps any one directory small in the file backend. The
// digest must be 64 lowercase hex characters and ns must pass validateNamespace
// (kept as defence in depth), so the result can never escape the blobs/ prefix.
func objectKey(ns namespace.Namespace, digest string) (string, error) {
	if err := checkNamespace(ns); err != nil {
		return "", err
	}
	if !validDigest(digest) {
		return "", fmt.Errorf("%w: digest %q is not 64 lowercase hex characters", ErrInvalidRef, digest)
	}
	nsseg := hex.EncodeToString([]byte(ns))
	return "blobs/" + nsseg + "/sha256/" + digest[0:2] + "/" + digest[2:4] + "/" + digest, nil
}
