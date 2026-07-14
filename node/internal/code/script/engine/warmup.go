package engine

import "context"

// Warmer is implemented by engine packages that can absorb a non-trivial
// cold-start cost (e.g. compiling embedded wasm). Engines self-register by
// calling RegisterWarmer in their init(). Warmers are invoked by Warmup.
//
// Engines with negligible cold start (goja, wazero) do not need to register;
// only those that benefit from up-front work (currently only js/qjs).
type Warmer func(ctx context.Context) error

var warmers []Warmer

// RegisterWarmer adds a warmer to the global list. Called from engine init().
// Not safe for concurrent registration after process start, which is fine —
// init() runs single-threaded.
func RegisterWarmer(w Warmer) { warmers = append(warmers, w) }

// Warmup runs every registered warmer sequentially. Servers/runners should
// call this once at startup to absorb engine cold-start latency before
// traffic arrives. Returns the first error encountered (rare; engines should
// log internally rather than fail startup).
func Warmup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for _, w := range warmers {
		if err := w(ctx); err != nil {
			return err
		}
	}
	return nil
}
