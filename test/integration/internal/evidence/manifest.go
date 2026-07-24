package evidence

// A0Scenario identifies one of the five required A0 fault scenarios.
type A0Scenario string

const (
	// A0CommitThenFlushBeforeDelivery verifies a commit is persisted before
	// delivery to the downstream consumer.
	A0CommitThenFlushBeforeDelivery A0Scenario = "CommitThenFlushBeforeDelivery"
	// A0ReportAckLoss verifies the runner report path tolerates an ACK loss.
	A0ReportAckLoss A0Scenario = "ReportAckLoss"
	// A0ReportRequestLoss verifies the runner report path tolerates a request loss.
	A0ReportRequestLoss A0Scenario = "ReportRequestLoss"
	// A0QueueHandoff verifies queue handoff preserves exactly-once commit semantics.
	A0QueueHandoff A0Scenario = "QueueHandoff"
	// A0OSKillSIGKILL verifies recovery after a real SIGKILL without a graceful path.
	A0OSKillSIGKILL A0Scenario = "OSKillSIGKILL"
)

// A0RequiredScenarios returns the five scenarios in canonical order.
// The verifier requires exactly one derived observation per scenario.
func A0RequiredScenarios() []A0Scenario {
	return []A0Scenario{
		A0CommitThenFlushBeforeDelivery,
		A0ReportAckLoss,
		A0ReportRequestLoss,
		A0QueueHandoff,
		A0OSKillSIGKILL,
	}
}

// A0ScenarioHandlerType returns the production handler identifier that each A0
// scenario's counter snapshot MUST declare under HandlerName. The verifier
// cross-checks the counter snapshot's HandlerName against this value so a bare
// counter with an arbitrary HandlerName cannot satisfy the
// handler_invocations>0 requirement — the counter must be bound to the
// scenario's actual production delegate.
//
// OSKillSIGKILL returns "" because it is phase-B direct-drive: it carries no
// counter snapshot (business handler_invocations is legitimately 0) and is
// instead required to show a system_task_delivery protocol observation per
// spec §3.5, so no HandlerName introspection applies.
func A0ScenarioHandlerType(s A0Scenario) string {
	switch s {
	case A0CommitThenFlushBeforeDelivery:
		return "test.fault"
	case A0ReportAckLoss, A0ReportRequestLoss:
		return "test.a0.start"
	case A0QueueHandoff:
		return "queue-handoff-consumer"
	case A0OSKillSIGKILL:
		return ""
	}
	return ""
}

// A3Fixture identifies a required error-handling fixture.
type A3Fixture string

const (
	// A3TransientThenSuccess verifies a transient error is retried and succeeds.
	A3TransientThenSuccess A3Fixture = "transient_then_success"
	// A3TransientRetryExhausted verifies a transient error exhausts retries.
	A3TransientRetryExhausted A3Fixture = "transient_retry_exhausted"
	// A3PermanentNoRetry verifies a permanent error is not retried.
	A3PermanentNoRetry A3Fixture = "permanent_no_retry"
	// A3BusinessErrorNoRetry verifies a business error routes to the error output.
	A3BusinessErrorNoRetry A3Fixture = "business_error_no_retry"
	// A3ErrorPortRetryExhausted verifies an error-port output exhausts retries.
	A3ErrorPortRetryExhausted A3Fixture = "error_port_retry_exhausted"
)

// A3Topology identifies where a fixture executes.
type A3Topology string

const (
	// A3Local runs the fixture in-process with fake/simulated backends.
	A3Local A3Topology = "local"
	// A3ServerRunner runs the fixture through the server-runner wire path.
	A3ServerRunner A3Topology = "server-runner"
	// A3ClusterDurable runs the fixture with durable Redis/MySQL backends.
	A3ClusterDurable A3Topology = "cluster-durable"
)

// A3MatrixRow is one cell of the required fixture × topology matrix.
type A3MatrixRow struct {
	Fixture  A3Fixture
	Topology A3Topology
	// DatabaseRealPair is true when this row is part of the Database real-MySQL
	// parity pair (server-runner ↔ cluster-durable). It is false for HTTP/gRPC
	// script fixtures and for Database local-fake.
	DatabaseRealPair bool
	// IsDatabaseLocalFake is true only for Database local-fake topology, which
	// has an independent contract and is not compared to the real pair.
	IsDatabaseLocalFake bool
}

// A3RequiredRows returns the full fixture × topology matrix required by A3.
// Database fixtures use the special contract described in spec §8.6:
// server-runner and cluster-durable form the real-MySQL parity pair; local-fake
// is a separate contract. Non-Database fixtures are required across all three
// topologies.
func A3RequiredRows() []A3MatrixRow {
	fixtures := []A3Fixture{
		A3TransientThenSuccess,
		A3TransientRetryExhausted,
		A3PermanentNoRetry,
		A3BusinessErrorNoRetry,
		A3ErrorPortRetryExhausted,
	}
	topologies := []A3Topology{A3Local, A3ServerRunner, A3ClusterDurable}

	var rows []A3MatrixRow
	for _, f := range fixtures {
		for _, t := range topologies {
			row := A3MatrixRow{Fixture: f, Topology: t}
			// Database fixtures are identified by name prefix "database_".
			if isDatabaseFixture(f) {
				switch t {
				case A3ServerRunner, A3ClusterDurable:
					row.DatabaseRealPair = true
				case A3Local:
					row.IsDatabaseLocalFake = true
				}
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func isDatabaseFixture(f A3Fixture) bool {
	// The five A3 fixtures are generic action parity fixtures. The Database
	// test package reuses them under the same names. For the required manifest
	// we treat these fixtures as participating in the Database real-pair
	// contract (server-runner ↔ cluster-durable) in addition to the generic
	// A3 contract; this ensures the verifier checks real-pair parity wherever
	// these fixtures execute under durable MySQL.
	return true
}

// A3DatabaseFixtures returns the Database fixture names that participate in the
// real-pair contract.
func A3DatabaseFixtures() []A3Fixture {
	return []A3Fixture{
		A3TransientThenSuccess,
		A3TransientRetryExhausted,
		A3PermanentNoRetry,
		A3BusinessErrorNoRetry,
		A3ErrorPortRetryExhausted,
	}
}

// Manifest bundles all required A0 and A3 entries. The verifier compares the
// derived observations against this manifest.
type Manifest struct {
	A0Scenarios []A0Scenario
	A3Rows      []A3MatrixRow
}

// DefaultManifest returns the canonical required manifest.
func DefaultManifest() *Manifest {
	return &Manifest{
		A0Scenarios: A0RequiredScenarios(),
		A3Rows:      A3RequiredRows(),
	}
}
