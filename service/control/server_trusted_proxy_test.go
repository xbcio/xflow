package control

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestServerSourceIPTrustedProxies(t *testing.T) {
	tests := []struct {
		name       string
		trusted    []string
		remoteAddr string
		xff        string
		want       string
	}{
		{
			name:       "unconfigured preserves legacy behavior",
			remoteAddr: "203.0.113.10:4321",
			xff:        "198.51.100.20",
			want:       "203.0.113.10",
		},
		{
			name:       "untrusted peer ignores forged xff",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "203.0.113.10:4321",
			xff:        "198.51.100.20, 10.0.0.2",
			want:       "203.0.113.10",
		},
		{
			name:       "rightmost untrusted address wins",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.3:4321",
			xff:        "192.0.2.1, 198.51.100.20, 10.0.0.2",
			want:       "198.51.100.20",
		},
		{
			name:       "all trusted falls back to leftmost",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.3:4321",
			xff:        "10.1.0.1, 10.2.0.2",
			want:       "10.1.0.1",
		},
		{
			name:       "parse failure falls back to leftmost legal address",
			trusted:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.3:4321",
			xff:        "192.0.2.1, not-an-ip, 10.0.0.2",
			want:       "192.0.2.1",
		},
		{
			name:       "ipv6 proxy chain",
			trusted:    []string{"fd00::/8"},
			remoteAddr: "[fd00::3]:4321",
			xff:        "2001:db8::1, fd00::2",
			want:       "2001:db8::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixes := make([]netip.Prefix, len(tt.trusted))
			for i, raw := range tt.trusted {
				prefixes[i] = netip.MustParsePrefix(raw)
			}
			opts := []ServerOption(nil)
			if tt.trusted != nil {
				opts = append(opts, WithTrustedProxies(prefixes))
			}
			server := NewServer(nil, NewMemoryRunnerDirectory(), opts...)
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			req.Header.Set("X-Forwarded-For", tt.xff)
			if got := server.httpTransportInfo(req).SourceIP; got != tt.want {
				t.Fatalf("SourceIP = %q, want %q", got, tt.want)
			}
		})
	}
}
