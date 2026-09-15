package control

import (
	"fmt"
	"strings"
)

// defaultEnrollmentRunnerIDPrefix preserves the identity shape issued before
// enrollment prefixes became configurable.
const defaultEnrollmentRunnerIDPrefix = "runner-"

// normalizeEnrollmentRunnerIDPrefix returns the effective prefix for an
// enrollment-issued runner ID. Empty input intentionally preserves the legacy
// runner- prefix. Non-empty values are restricted to a small identifier-safe
// alphabet because the value becomes part of an externally visible identity
// and is later enforced by RunnerPolicy.IDPrefix.
func normalizeEnrollmentRunnerIDPrefix(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return defaultEnrollmentRunnerIDPrefix, nil
	}
	if len(prefix) > 64 {
		return "", fmt.Errorf("control: enrollment runner ID prefix must be at most 64 bytes")
	}
	for _, r := range prefix {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return "", fmt.Errorf("control: enrollment runner ID prefix may contain only letters, digits, '-', '_' or '.'")
	}
	return prefix, nil
}

func (c *Core) enrollmentRunnerIDPrefixOrDefault() string {
	if c != nil && c.enrollmentRunnerIDPrefix != "" {
		return c.enrollmentRunnerIDPrefix
	}
	return defaultEnrollmentRunnerIDPrefix
}
