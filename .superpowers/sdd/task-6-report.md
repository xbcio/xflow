# Task 6 Report

Status: DONE

Summary:
- Kept the Task 5 shared `RunnerDirectory` poll path intact and finished Task 6 by fencing `reportResult` with live runner sessions before engine commit, while leaving dispatcher assignment-only.
- Mapped stale runner sessions through the transports as required: HTTP now returns `409 Conflict`, and gRPC now returns `FailedPrecondition`.
- Extended directory lease cleanup to be lease-precise: `ReleaseLeased` now resolves finalized assignments by lease token/lease ID, releases matching stale leases, and refuses to drop a replacement live lease or clear `seen` incorrectly.
- Added HTTP and gRPC regressions for stale sessions, stale-token cleanup, and the internal-task-identity case where transport payloads omit hidden task fields.

Tests run:
- `go test ./service/control -run 'TestHTTPPollRejectsStaleSession|TestHTTPReportResultRejectsStaleLeaseToken|TestHTTPReportResultStaleTokenReleasesMatchingLeaseCapacity|TestHTTPReportResultStaleTokenReleasesCapacityForInternalTaskIdentity|TestHTTPReportResultStaleTokenKeepsReplacementLeaseAndSeen|TestGRPCReportResultRejectsStaleLeaseToken|TestGRPCReportResultRejectsStaleSession' -count=1`
- `go test ./service/control ./cmd/server -count=1`

Concerns:
- None.
