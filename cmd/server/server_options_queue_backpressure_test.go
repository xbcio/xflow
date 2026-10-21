package main

import (
	"reflect"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// TestBuildServerOptionsWiresQueueBackpressure covers the hop the parse tests
// cannot: buildServerOptions actually attaching cfg.queueMaxDepth and
// cfg.queueDepthSampleInterval to xflowsdk.WithServerQueueBackpressure rather
// than dropping them or picking up a same-typed sibling field.
//
// There is no exported accessor for the SDK's assembled apiserver.Config (the
// SDK's serverConfig is unexported), so the options are applied through
// reflection to a zero-value config of the type the options themselves accept
// — exactly what NewServer does — and the two fields are read back. The field
// names are unexported, so a rename fails this test loudly instead of letting
// it pass vacuously. The SDK-side hop from that option to apiserver.Config is
// already pinned by sdk/xflow's TestWithServerQueueBackpressureReachesTheAPIConfig.
func TestBuildServerOptionsWiresQueueBackpressure(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantMaxDepth int
		wantInterval time.Duration
	}{
		{
			name:         "zero disables the bound",
			args:         nil,
			wantMaxDepth: 0,
			wantInterval: time.Second,
		},
		{
			name:         "flags reach the option",
			args:         []string{"-queue-max-depth", "4096", "-queue-depth-sample-interval", "250ms"},
			wantMaxDepth: 4096,
			wantInterval: 250 * time.Millisecond,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseServerConfig(append([]string{"-mode", "dev", "-memory"}, tc.args...))
			if err != nil {
				t.Fatalf("parseServerConfig: %v", err)
			}
			gotMaxDepth, gotInterval := applyServerOptions(t, buildServerOptions(cfg, serverDeps{}))
			if gotMaxDepth != tc.wantMaxDepth {
				t.Fatalf("queueMaxDepth = %d, want %d", gotMaxDepth, tc.wantMaxDepth)
			}
			if gotInterval != tc.wantInterval {
				t.Fatalf("queueDepthInterval = %v, want %v", gotInterval, tc.wantInterval)
			}
		})
	}
}

// applyServerOptions applies opts the way xflowsdk.NewServer does and returns
// the two queue-backpressure fields of the resulting SDK serverConfig.
func applyServerOptions(t *testing.T, opts []xflowsdk.ServerOption) (int, time.Duration) {
	t.Helper()
	if len(opts) == 0 {
		t.Fatal("buildServerOptions returned no options")
	}
	optType := reflect.TypeOf(opts[0])
	if optType.Kind() != reflect.Func || optType.NumIn() != 1 {
		t.Fatalf("ServerOption type = %v, want a func with one parameter", optType)
	}
	sc := reflect.New(optType.In(0).Elem())
	for _, opt := range opts {
		reflect.ValueOf(opt).Call([]reflect.Value{sc})
	}
	fields := sc.Elem()
	maxDepth := fields.FieldByName("queueMaxDepth")
	interval := fields.FieldByName("queueDepthInterval")
	if !maxDepth.IsValid() || !interval.IsValid() {
		t.Fatal("SDK serverConfig no longer has queueMaxDepth/queueDepthInterval fields; update this wiring test")
	}
	return int(maxDepth.Int()), time.Duration(interval.Int())
}
