package script

import "encoding/base64"

// TODO($helpers): expand the non-security helper surface. Candidates:
//
//   - crypto:   hmac(alg, key, msg), sha256(s), sha1(s), md5(s)
//   - encoding: hex.encode/decode, urlEncode/Decode, jsonEncode/Decode
//   - jsonpath: jsonPath(obj, path) -> any
//   - time:     dateFormat(t, layout), parseTime(s, layout), now() (caveat:
//               clock is non-deterministic; expose only when needed and
//               document the determinism break)
//   - regex:    regexMatch(s, pattern), regexReplace(s, pat, repl)
//
// Each addition MUST be:
//   1. Deterministic AND I/O-free (anything with side effects belongs in a
//      dedicated node — HTTP / SQL / etc., not in $helpers).
//   2. Mirrored across BOTH the goja and qjs runtime injection sites so the
//      two language-family runtimes stay observationally identical.
//   3. Covered by a parity test that asserts the same input yields the same
//      result across goja, qjs, and (where applicable) wasm guests.
//
// When the helper set grows past ~10 functions, split Helpers into thematic
// sub-interfaces (CryptoHelpers, EncodingHelpers, ...) instead of one
// god-interface, so engine layers can compose narrower capabilities.

type defaultHelpers struct{}

// DefaultHelpers returns the standard non-security helper set.
func DefaultHelpers() Helpers { return defaultHelpers{} }

func (defaultHelpers) Base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func (defaultHelpers) Base64Decode(s string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
