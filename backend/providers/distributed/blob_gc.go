package distributed

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store/blob"
)

const (
	// gcDefaultTick is how long the loop waits between ticks.
	gcDefaultTick = 5 * time.Minute
	// gcDefaultBatch matches the Batch a blob.Collector is normally built with.
	gcDefaultBatch = 200
	// gcDefaultMaxRuns caps the back-to-back passes one tick may run.
	gcDefaultMaxRuns = 10
)

// gcObserver receives the outcome of each blob collection pass. Skipped passes
// are only logged, so they are not part of the surface; a pass that failed to
// delete objects is reported to an observer that also implements
// gcFailureObserver.
type gcObserver interface {
	OnGC(deleted, fenced int)
}

// gcFailureObserver is the optional extension of gcObserver that receives a
// pass's failures: the objects it could not delete, plus one for a pass that
// failed as a whole. It is separate from gcObserver so adding it cannot break an
// existing observer, and a pass with no failure is not reported.
type gcFailureObserver interface {
	OnGCFailed(failed int)
}

// blobGCOptions configures startBlobGC. Every field is optional.
type blobGCOptions struct {
	// Tick is the wait between ticks. Zero means gcDefaultTick.
	Tick time.Duration
	// Batch is the Batch the Collector behind run was built with. A pass that
	// examined exactly Batch candidates may have left more behind, so it is
	// followed by another at once. Zero means gcDefaultBatch.
	Batch int
	// MaxRunsPerTick caps the passes one tick may chain. Zero means
	// gcDefaultMaxRuns.
	MaxRunsPerTick int
	// Sleep waits d or until ctx ends, returning ctx's error in that case. It is
	// the loop's only clock; nil means a real timer. Tests inject a fake one.
	Sleep func(ctx context.Context, d time.Duration) error
	// Observer is told the outcome of every pass. Nil is allowed.
	Observer gcObserver
	// Logger receives the loop's log lines; nil falls back to the stdlib log.
	Logger engine.Logger
	// onExit, when set, runs on the loop's goroutine as its last act, whatever
	// ended the loop, before stop is released. Tests use it to see the loop end.
	onExit func()
}

// withDefaults fills the zero fields.
func (o blobGCOptions) withDefaults() blobGCOptions {
	if o.Tick <= 0 {
		o.Tick = gcDefaultTick
	}
	if o.Batch <= 0 {
		o.Batch = gcDefaultBatch
	}
	if o.MaxRunsPerTick <= 0 {
		o.MaxRunsPerTick = gcDefaultMaxRuns
	}
	if o.Sleep == nil {
		o.Sleep = gcSleep
	}
	return o
}

// startBlobGC runs run on a timer in its own goroutine until the returned stop
// is called or parent ends. run is one Collector pass. The first pass happens
// after one Tick, not at start, so a restart does not pay for a pass.
//
// stop cancels the loop's context, which run receives, and waits for the
// goroutine to exit, so no pass is still running once it returns. It is safe to
// call more than once and from several goroutines. The loop can also end by
// itself (see gcTick); stop remains safe to call afterwards.
func startBlobGC(parent context.Context, run func(context.Context) (blob.CollectResult, error), opts blobGCOptions) (stop func()) {
	opts = opts.withDefaults()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if opts.onExit != nil {
			defer opts.onExit()
		}
		gcLoop(ctx, run, opts)
	}()
	return func() {
		cancel()
		<-done
	}
}

// gcLoop ticks until ctx ends or gcTick asks it to give up.
func gcLoop(ctx context.Context, run func(context.Context) (blob.CollectResult, error), o blobGCOptions) {
	for ctx.Err() == nil {
		if err := o.Sleep(ctx, o.Tick); err != nil {
			return
		}
		if !gcTick(ctx, run, o) {
			return
		}
	}
}

// gcTick runs one tick's passes and reports whether the loop should go on.
//
// A pass that examined a full Batch is followed at once by another, at most
// MaxRunsPerTick in a row, so one tick cannot monopolise the Index. An error
// ends the tick without a follow-up pass: the objects that failed are listed
// again by the very next pass and would only fail again. Only an error that
// says the store cannot delete at all ends the loop, since no later pass could
// do anything else.
func gcTick(ctx context.Context, run func(context.Context) (blob.CollectResult, error), o blobGCOptions) bool {
	for i := 0; i < o.MaxRunsPerTick; i++ {
		if ctx.Err() != nil {
			return true
		}
		res, err := run(ctx)
		if o.Observer != nil {
			o.Observer.OnGC(res.Deleted, res.Fenced)
			if fo, ok := o.Observer.(gcFailureObserver); ok {
				failed := res.Failed
				// A pass cut short by shutdown is not a failure of the store.
				if err != nil && ctx.Err() == nil {
					failed++
				}
				if failed > 0 {
					fo.OnGCFailed(failed)
				}
			}
		}
		gcLogCounts(o.Logger, res)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return true // shutting down: the pass was cut short on purpose
		case errors.Is(err, blob.ErrDeleteUnsupported):
			gcLogError(o.Logger, "blob gc stopped: the blob store cannot delete objects", err)
			return false
		default:
			gcLogError(o.Logger, "blob gc pass failed", err)
			return true
		}
		if res.Candidates != o.Batch {
			return true
		}
	}
	return true
}

// gcSleep waits d, or until ctx ends.
func gcSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// gcLogError reports a failed pass. The error is sanitized because a store or
// index error can carry a connection string.
func gcLogError(logger engine.Logger, msg string, err error) {
	if logger != nil {
		logger.Error("distributed backend: "+msg, "error", sanitizeLogError(err))
		return
	}
	log.Printf("xflow: distributed backend %s: %v", msg, sanitizeLogError(err))
}

// gcLogCounts records a pass's skipped and failed counts, and only when there is
// something to say.
func gcLogCounts(logger engine.Logger, res blob.CollectResult) {
	if res.Skipped == 0 && res.Failed == 0 {
		return
	}
	if logger != nil {
		logger.Info("distributed backend: blob gc pass", "skipped", res.Skipped, "failed", res.Failed)
		return
	}
	log.Printf("xflow: distributed backend blob gc pass: skipped=%d failed=%d", res.Skipped, res.Failed)
}
