package action

import "testing"

func TestHostPolicyPatternBoundariesAndDenyPriority(t *testing.T) {
	policy := NewHostPolicy(
		[]string{"exact.only.test", ".suffix.test", "*.wild.test", ".example.com"},
		[]string{".blocked.example.com"},
	)
	if policy == nil {
		t.Fatal("configured host policy is nil")
	}

	for _, tt := range []struct {
		host string
		want bool
	}{
		{host: "EXACT.ONLY.TEST", want: true},
		{host: "api.exact.only.test", want: false},
		{host: "suffix.test", want: true},
		{host: "api.suffix.test", want: true},
		{host: "deep.api.suffix.test", want: true},
		{host: "evil-suffix.test", want: false},
		{host: "wild.test", want: false},
		{host: "api.wild.test", want: true},
		{host: "deep.api.wild.test", want: true},
		{host: "evil-wild.test", want: false},
		{host: "example.com", want: true},
		{host: "api.example.com", want: true},
		{host: "evil-example.com", want: false},
		{host: "blocked.example.com", want: false},
		{host: "api.blocked.example.com", want: false},
	} {
		t.Run(tt.host, func(t *testing.T) {
			if got := policy(tt.host) == nil; got != tt.want {
				t.Fatalf("policy(%q) allowed = %t, want %t", tt.host, got, tt.want)
			}
		})
	}
}

func TestHostPolicyInvalidRulesFailClosed(t *testing.T) {
	for _, rule := range []string{
		"https://example.com",
		"example.com:443",
		"example.com/path",
		"user@example.com",
		"*.example.com:443",
		"*.user@example.com",
		".127.0.0.1",
		"*.127.0.0.1",
	} {
		t.Run(rule, func(t *testing.T) {
			policy := NewHostPolicy([]string{rule}, nil)
			if policy == nil {
				t.Fatal("invalid configured policy is nil")
			}
			if err := policy("allowed.test"); err == nil {
				t.Fatalf("invalid allow rule %q did not fail closed", rule)
			}
		})
	}

	policy := NewHostPolicy([]string{"allowed.test"}, []string{"blocked.test:443"})
	if policy == nil {
		t.Fatal("invalid deny rule produced nil policy")
	}
	if err := policy("allowed.test"); err == nil {
		t.Fatal("invalid deny rule did not fail closed")
	}
	if NewHostPolicy(nil, nil) != nil {
		t.Fatal("empty lists must preserve the nil policy default")
	}
}
