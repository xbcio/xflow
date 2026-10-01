package control

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// LiveRunnerNodeTypes returns the node descriptors the live runner fleet
// reports for ns, aggregated by (type, version) — the runner-reported part
// of a namespace-scoped GET /v1/node-types. Records are filtered to ns before
// aggregating, so a conflict winner and the pool list only ever come from
// runners that namespace may see. Conflicts are logged and gauged from an
// aggregation of the unfiltered fleet instead, so the gauge reads the same
// whichever namespace was requested.
//
// The fleet-wide read is cached for runnerNodeTypesCacheTTL against now (see
// runnerNodeTypesCache), and the conflict log and gauge run once per read,
// not once per request. A registration or expiry therefore shows up within
// that TTL rather than on the very next request.
//
// It reads the raw runner directory, not RunnerDirectory(): the management
// decorator installed when metrics are configured does not forward optional
// capabilities such as RunnerDescriptorDirectory. A directory without that
// capability reports nothing. now is the liveness instant under
// DefaultRunnerLiveTTL.
func (cp *ControlPlane) LiveRunnerNodeTypes(ctx context.Context, ns namespace.Namespace, now time.Time) ([]AggregatedDescriptor, error) {
	dir, ok := cp.runners.(RunnerDescriptorDirectory)
	if !ok || dir == nil {
		return nil, nil
	}
	records, err := cp.runnerNodeTypes.get(ctx, now,
		func(ctx context.Context) ([]RunnerDescriptorRecord, error) {
			return dir.LiveRunnerDescriptors(ctx, now)
		},
		func(ctx context.Context, records []RunnerDescriptorRecord) {
			cp.runnerDescriptorConflicts.report(ctx, AggregateRunnerDescriptors(records))
		})
	if err != nil {
		return nil, err
	}
	return AggregateRunnerDescriptors(FilterRunnerDescriptorRecords(records, ns)), nil
}
