package timer

import (
	"github.com/xbcio/xflow/node/trigger/internal/fireobs"
	"github.com/xbcio/xflow/types"
)

// fireDrops counts the ticks this trigger decided not to admit. The counter
// itself lives in observability/metrics and is installed by the process builder
// (sdk/xflow, next to the kafka trigger's observer); without one the timer still
// ticks and still drops, it just reports nothing.
var fireDrops fireobs.Counter

// SetObserver installs the counter dropped ticks are reported to. Passing nil
// removes it. See fireobs.Counter.Set for why a second install is a panic.
func SetObserver(o types.TriggerFireObserver) { fireDrops.Set(o) }
