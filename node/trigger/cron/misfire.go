package cron

import (
	"fmt"
	"time"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/node/internal/utils/conv"
)

// misfirePolicy is what a cron trigger does about the scheduled instants that
// passed while nothing was hosting its schedule.
//
// A schedule is a stream of named moments, and a moment that passed unhosted is
// a decision the operator makes once, in the definition, rather than one the
// trigger makes per restart:
//
//   - skip acknowledges the missed instants and runs none of them. A stale
//     moment is not worth running: the work a periodic trigger names is usually
//     "keep the state fresh", and fifty runs of it now is worse than one.
//   - fire_once runs the most recent missed instant. The schedule was due many
//     times and the work needed doing at least once.
//   - fire_all runs them, oldest first, bounded by the catch-up window and the
//     per-activation cap. This is for a schedule whose every moment is a
//     distinct piece of work (a daily close, an hourly settlement).
type misfirePolicy string

const (
	misfireSkip     misfirePolicy = "skip"
	misfireFireOnce misfirePolicy = "fire_once"
	misfireFireAll  misfirePolicy = "fire_all"
)

// The values an absent param resolves to. They are declared in
// node/internal.BuiltinFallbacks rather than as ParamSpec.Default: a Default is
// written into a workflow's params by the SDK, and adding one moves the
// definition hash of every workflow using this node.
const (
	defaultCatchupWindow = time.Hour
	defaultMaxCatchup    = 10
)

// catchupConfig is one activation's catch-up settings, resolved once from the
// params so the planner stays pure and every caller sees the same numbers.
type catchupConfig struct {
	policy misfirePolicy
	// window is how far back a catch-up may reach. Instants older than it are
	// not fired, and are not even examined.
	window time.Duration
	// max is the most firings one activation may catch up.
	max int
}

// catchupConfigFrom parses the three catch-up params. An absent param is the
// documented fallback; a set one that cannot be read is an error rather than a
// silent fallback, because the value was written on purpose — a typo in
// "catchup_window" must not turn a fire_all schedule into a skip schedule
// without saying so. The descriptor's Enum/Format checks catch the same values
// at compile time; this is the boundary that also covers a definition compiled
// before these params existed.
func catchupConfigFrom(params map[string]any) (catchupConfig, error) {
	cfg := catchupConfig{window: defaultCatchupWindow, max: defaultMaxCatchup}
	policy, err := parseMisfirePolicy(params["misfire"])
	if err != nil {
		return catchupConfig{}, err
	}
	cfg.policy = policy
	if raw := params["catchup_window"]; raw != nil && cast.ToString(raw) != "" {
		window, err := conv.PositiveDuration(raw)
		if err != nil {
			return catchupConfig{}, fmt.Errorf("catchup_window: %w", err)
		}
		cfg.window = window
	}
	if raw := params["max_catchup"]; raw != nil && cast.ToString(raw) != "" {
		max, err := cast.ToIntE(raw)
		if err != nil || max <= 0 {
			return catchupConfig{}, fmt.Errorf("max_catchup must be a positive integer")
		}
		cfg.max = max
	}
	return cfg, nil
}

// parseMisfirePolicy maps the param value to a policy. An absent value is the
// default (skip); an unknown one is an error, for the same reason a malformed
// window is.
func parseMisfirePolicy(v any) (misfirePolicy, error) {
	policy := misfirePolicy(cast.ToString(v))
	switch policy {
	case "":
		return misfireSkip, nil
	case misfireSkip, misfireFireOnce, misfireFireAll:
		return policy, nil
	default:
		return "", fmt.Errorf("misfire must be one of: %s, %s, %s", misfireSkip, misfireFireOnce, misfireFireAll)
	}
}
