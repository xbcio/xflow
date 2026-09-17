package action

import (
	"errors"
	"net"
	"sort"
	"strings"
)

type hostPatternKind uint8

const (
	hostPatternExact hostPatternKind = iota
	hostPatternSuffix
	hostPatternWildcard
)

type hostPattern struct {
	kind hostPatternKind
	host string
}

// parseHostPattern accepts an exact hostname, a suffix hostname prefixed by
// '.', or a wildcard hostname prefixed by '*.'. It deliberately keeps the
// suffix and wildcard forms distinct: a suffix includes its apex and a
// wildcard requires at least one additional DNS label.
func parseHostPattern(raw string) (hostPattern, error) {
	pattern := hostPattern{kind: hostPatternExact, host: raw}
	switch {
	case strings.HasPrefix(raw, "*."):
		pattern.kind = hostPatternWildcard
		pattern.host = raw[2:]
	case strings.HasPrefix(raw, "."):
		pattern.kind = hostPatternSuffix
		pattern.host = raw[1:]
	}

	host, err := normalizeHostOnly(pattern.host)
	if err != nil {
		return hostPattern{}, err
	}
	if pattern.kind != hostPatternExact && net.ParseIP(host) != nil {
		return hostPattern{}, errors.New("host patterns cannot use IP literals")
	}
	pattern.host = host
	return pattern, nil
}

func (p hostPattern) String() string {
	switch p.kind {
	case hostPatternSuffix:
		return "." + p.host
	case hostPatternWildcard:
		return "*." + p.host
	default:
		return p.host
	}
}

func (p hostPattern) matches(normalizedHost string) bool {
	switch p.kind {
	case hostPatternExact:
		return normalizedHost == p.host
	case hostPatternSuffix:
		return normalizedHost == p.host || strings.HasSuffix(normalizedHost, "."+p.host)
	case hostPatternWildcard:
		return normalizedHost != p.host && strings.HasSuffix(normalizedHost, "."+p.host)
	default:
		return false
	}
}

func compileHostPatterns(raw []string) ([]hostPattern, error) {
	patternsByValue := make(map[string]hostPattern, len(raw))
	for _, value := range raw {
		pattern, err := parseHostPattern(value)
		if err != nil {
			return nil, err
		}
		patternsByValue[pattern.String()] = pattern
	}
	patterns := make([]hostPattern, 0, len(patternsByValue))
	for _, pattern := range patternsByValue {
		patterns = append(patterns, pattern)
	}
	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].String() < patterns[j].String()
	})
	return patterns, nil
}

func normalizedHostPatterns(raw []string) ([]string, error) {
	patterns, err := compileHostPatterns(raw)
	if err != nil {
		return nil, err
	}
	values := make([]string, len(patterns))
	for i, pattern := range patterns {
		values[i] = pattern.String()
	}
	return values, nil
}

func hostPatternsMatch(patterns []hostPattern, host string) bool {
	normalizedHost, err := normalizeHostOnly(host)
	if err != nil {
		return false
	}
	for _, pattern := range patterns {
		if pattern.matches(normalizedHost) {
			return true
		}
	}
	return false
}

func normalizeHostOnly(raw string) (string, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "/\\?#@") || strings.ContainsAny(raw, " \t\r\n") {
		return "", errors.New("invalid host")
	}
	host := strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
	if strings.Contains(host, "[") || strings.Contains(host, "]") {
		return "", errors.New("invalid host")
	}
	if ip := net.ParseIP(host); ip != nil {
		return strings.ToLower(ip.String()), nil
	}
	if strings.Contains(host, ":") || len(host) > 253 {
		return "", errors.New("invalid host")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", errors.New("invalid host")
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
				return "", errors.New("invalid host")
			}
		}
	}
	return strings.ToLower(host), nil
}
