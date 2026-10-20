import { describe, expect, it } from "vitest";
import {
  ALL_CAPABILITIES,
  capabilitiesFromScopes,
  missingCapabilityScopes,
  type XFlowEditorCapabilities
} from "./capabilities";

// Each row is one operation family of `service/apiserver/authz.go`
// `scopeForOperation`, pinned so the frontend projection cannot drift from the
// server's scope vocabulary. `execution.control` and `execution.rerun` are
// separate grants on purpose: neither implies `execution`.
const allFalse: XFlowEditorCapabilities = {
  workflow: false,
  execution: false,
  executionControl: false,
  executionRerun: false
};

const cases: Array<{
  name: string;
  scopes: readonly string[] | null | undefined;
  expected: XFlowEditorCapabilities;
}> = [
  { name: "grants nothing for an empty list", scopes: [], expected: allFalse },
  {
    name: "treats an absent scope list as no grant (fail-closed)",
    scopes: undefined,
    expected: allFalse
  },
  {
    name: "treats a null scope list as no grant",
    scopes: null,
    expected: allFalse
  },
  {
    name: "maps workflow to definition read/write, publish, rollback and run",
    scopes: ["workflow"],
    expected: { ...allFalse, workflow: true }
  },
  {
    name: "maps execution to read, cancel, signal and revoke",
    scopes: ["execution"],
    expected: { ...allFalse, execution: true }
  },
  {
    name: "maps execution.control to pause/resume without granting execution",
    scopes: ["execution.control"],
    expected: { ...allFalse, executionControl: true }
  },
  {
    name: "maps execution.rerun to rerun alone",
    scopes: ["execution.rerun"],
    expected: { ...allFalse, executionRerun: true }
  },
  {
    name: "ignores scopes the editor has no capability for",
    scopes: ["management.read", "deadletter.list", "schedule.read"],
    expected: allFalse
  },
  {
    name: "ignores unknown values mixed with a known scope",
    scopes: ["unknown.scope", "workflow", "workflow"],
    expected: { ...allFalse, workflow: true }
  },
  {
    name: "grants every bit when the credential carries all four scopes",
    scopes: ["workflow", "execution", "execution.control", "execution.rerun"],
    expected: {
      workflow: true,
      execution: true,
      executionControl: true,
      executionRerun: true
    }
  }
];

describe("capabilitiesFromScopes", () => {
  for (const { name, scopes, expected } of cases) {
    it(name, () => {
      expect(capabilitiesFromScopes(scopes)).toEqual(expected);
    });
  }

  it("returns a fresh object each call so callers cannot poison the mapping", () => {
    const first = capabilitiesFromScopes(["workflow"]);
    first.workflow = false;
    expect(capabilitiesFromScopes(["workflow"]).workflow).toBe(true);
  });
});

describe("ALL_CAPABILITIES", () => {
  it("grants all four bits and is frozen", () => {
    expect(ALL_CAPABILITIES).toEqual({
      workflow: true,
      execution: true,
      executionControl: true,
      executionRerun: true
    });
    expect(Object.isFrozen(ALL_CAPABILITIES)).toBe(true);
  });
});

describe("missingCapabilityScopes", () => {
  it("names nothing when everything is granted", () => {
    expect(missingCapabilityScopes({ ...ALL_CAPABILITIES })).toEqual([]);
  });

  it("names only the workflow scope when the credential cannot write", () => {
    expect(
      missingCapabilityScopes({ workflow: false, execution: true, executionControl: true, executionRerun: true })
    ).toEqual(["workflow"]);
  });

  it("keeps the authz vocabulary and a stable order when several are missing", () => {
    expect(missingCapabilityScopes({ workflow: true, execution: false, executionControl: false, executionRerun: false })).toEqual([
      "execution",
      "execution.control",
      "execution.rerun"
    ]);
    expect(missingCapabilityScopes(allFalse)).toEqual([
      "workflow",
      "execution",
      "execution.control",
      "execution.rerun"
    ]);
  });
});
