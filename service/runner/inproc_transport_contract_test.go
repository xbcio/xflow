package runner

import (
	"github.com/xbcio/xflow/service/control"
)

// The in-process runner transport is consumed through these interfaces, three
// of which are unexported here and asserted only structurally at the call sites
// in runner.go. A method that misses one of those signatures fails silently at
// runtime — the optional capability is simply never detected, so renewal, ack
// and metrics reporting quietly stop working. Asserting them here makes the
// omission a compile error instead.
var (
	_ ProtocolClient      = (*control.InProcessRunnerClient)(nil)
	_ leaseRenewClient    = (*control.InProcessRunnerClient)(nil)
	_ activationAckClient = (*control.InProcessRunnerClient)(nil)
	_ MetricsReportClient = (*control.InProcessRunnerClient)(nil)
)
