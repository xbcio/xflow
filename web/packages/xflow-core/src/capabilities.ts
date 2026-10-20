/**
 * The four capability bits the editor gates its surfaces on, derived from the
 * scopes GET /v1/current-user reports.
 *
 * The mapping mirrors `service/apiserver/authz.go` `scopeForOperation` exactly:
 * one scope per operation family, with no implication between them.
 *
 * - `workflow`          — create / read / register / update / invoke.
 * - `execution`         — read / signal / revoke / cancel / seed.
 * - `execution.control` — pause / resume, deliberately not `execution`.
 * - `execution.rerun`   — rerun from a terminal execution.
 *
 * `scopeForOperation` also answers unrelated scopes (`management.read`,
 * `deadletter.list`, `schedule.read` …). They are not editor capabilities and
 * are ignored here rather than guessed at.
 */
export interface XFlowEditorCapabilities {
  /** Definition read/write, publish, rollback and starting a run. */
  workflow: boolean;
  /** Reading executions, cancelling one, and delivering/revoking signals. */
  execution: boolean;
  /** Pausing and resuming a running execution. */
  executionControl: boolean;
  /** Re-running from a terminal execution. */
  executionRerun: boolean;
}

/**
 * Every bit granted. The editor treats an absent `capabilities` prop as this
 * value, so a host written before the prop existed keeps its P2 behavior
 * instead of silently losing controls.
 */
export const ALL_CAPABILITIES: Readonly<XFlowEditorCapabilities> = Object.freeze({
  workflow: true,
  execution: true,
  executionControl: true,
  executionRerun: true
});

/** Capability bit → the server scope it is derived from. */
const CAPABILITY_SCOPES: ReadonlyArray<readonly [keyof XFlowEditorCapabilities, string]> = [
  ["workflow", "workflow"],
  ["execution", "execution"],
  ["executionControl", "execution.control"],
  ["executionRerun", "execution.rerun"]
];

/**
 * Projects a credential's scope list onto the four editor capability bits.
 *
 * Unknown scopes are ignored and a missing/empty list grants nothing: the
 * projection is fail-closed because a caller that cannot name its scopes has
 * not been granted any. The bits stay independent on purpose —
 * `execution.control` alone is exactly the grant `authz.go` defines for
 * pause/resume, and it never implies `execution`.
 */
export function capabilitiesFromScopes(
  scopes: readonly string[] | null | undefined
): XFlowEditorCapabilities {
  const granted = new Set(scopes ?? []);
  return {
    workflow: granted.has("workflow"),
    execution: granted.has("execution"),
    executionControl: granted.has("execution.control"),
    executionRerun: granted.has("execution.rerun")
  };
}

/**
 * The scope names a capability object is missing, in field-declaration order.
 * Hosts use it to explain a read-only editor without asking the server again.
 */
export function missingCapabilityScopes(capabilities: XFlowEditorCapabilities): string[] {
  return CAPABILITY_SCOPES.filter(([capability]) => !capabilities[capability]).map(
    ([, scope]) => scope
  );
}
