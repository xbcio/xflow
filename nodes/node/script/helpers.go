package script

import "encoding/base64"

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
