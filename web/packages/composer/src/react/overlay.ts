// Overlay and microtask batching (Doc B §6 rule 6).
//
// - Writes inside one microtask are merged (same path: last wins, via core
//   createPatchBatch/mergePatches) into a single onChange call.
// - The overlay is every patch not yet acknowledged by the host: emitted
//   patches (sent through onChange, in order) plus unflushed ones. Resolve
//   and later writes see "host value + overlay", so rapid edits never
//   compute from stale data.
// - A new host `value` reference acknowledges emitted patches in order: find
//   the newest emitted patch the value already reflects (set: the value at
//   its path deep-equals the patch value; unset: the path is absent) and drop
//   it together with every patch emitted before it. The remaining emitted
//   patches and the unflushed ones are replayed on top of the new value, so a
//   lagging or batching host never rolls back newer keystrokes.
// - The timeout runs from the latest unacknowledged flush. If it passes
//   OVERLAY_TIMEOUT_MS without an acknowledgement covering every emitted
//   patch, all emitted patches are dropped with a warning (the host value
//   wins, e.g. for a host that rejects or normalizes them); only unflushed
//   patches are kept.

import { applyPatches, createPatchBatch, deepEqual, getPointer, write, type BindTarget, type Patch, type ResolvedTree, type ValueKind, type Warning } from "../core";

export const OVERLAY_TIMEOUT_MS = 1000;

export interface OverlayHost {
  onChange(patches: Patch[]): void;
  onWarning(warning: Warning): void;
  /** Asks the owning component to re-render with the new effective value. */
  invalidate(): void;
}

export class OverlayController {
  private base: object | undefined;
  private effective: object | undefined;
  private readonly batch = createPatchBatch();
  private unflushed: Patch[] = [];
  /** Patches sent through onChange that the host has not reflected yet, in order. */
  private emitted: Patch[] = [];
  private scheduled = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = true;

  /** Latest resolve result: repeat row mapping and object-bound targets for write(). */
  tree: Pick<ResolvedTree, "repeats" | "objectTargets"> | undefined;

  constructor(private host: OverlayHost) {}

  setHost(host: OverlayHost): void {
    this.host = host;
  }

  /** Returns the effective value for `hostValue`, rebasing when it is a new reference. */
  sync(hostValue: object): object {
    if (hostValue !== this.base) {
      this.base = hostValue;
      let acked = this.emitted.length - 1;
      while (acked >= 0 && !reflects(hostValue, this.emitted[acked])) acked--;
      if (acked >= 0) this.emitted = this.emitted.slice(acked + 1);
      if (this.emitted.length === 0) this.clearTimer();
      this.rebase();
    }
    return this.effective!;
  }

  get value(): object | undefined {
    return this.effective;
  }

  write(target: BindTarget, next: unknown, valueKind: ValueKind, emptyAs: "unset" | "keep" = "unset"): void {
    if (this.effective === undefined) return;
    const patches = write(target, next, valueKind, {
      value: this.effective,
      tree: this.tree,
      emptyAs,
      onWarning: (warning) => this.host.onWarning(warning)
    });
    if (patches.length === 0) return;
    this.effective = applyPatches(this.effective, patches);
    this.batch.push(...patches);
    this.unflushed.push(...patches);
    if (!this.scheduled) {
      this.scheduled = true;
      queueMicrotask(() => this.flush());
    }
    if (this.active) this.host.invalidate();
  }

  /** Emits the pending batch now (normally called from the scheduled microtask). */
  flush(): void {
    this.scheduled = false;
    this.unflushed = [];
    const patches = this.batch.flush();
    if (patches.length === 0) return;
    this.emitted.push(...patches);
    this.clearTimer();
    if (this.active) this.timer = setTimeout(() => this.expire(), OVERLAY_TIMEOUT_MS);
    this.host.onChange(patches);
  }

  /** Re-enables the timeout/invalidation after a (StrictMode) remount. */
  activate(): void {
    this.active = true;
    if (this.emitted.length > 0 && this.timer === undefined) {
      this.timer = setTimeout(() => this.expire(), OVERLAY_TIMEOUT_MS);
    }
  }

  /** Stops timers and re-render requests; pending patches are still emitted. */
  deactivate(): void {
    this.active = false;
    this.clearTimer();
  }

  private expire(): void {
    this.timer = undefined;
    if (this.emitted.length === 0 || this.base === undefined) return;
    this.emitted = [];
    this.host.onWarning({
      code: "overlay-timeout",
      message: `host did not acknowledge onChange within ${OVERLAY_TIMEOUT_MS}ms of the latest flush; dropping the local overlay`
    });
    this.rebase();
    if (this.active) this.host.invalidate();
  }

  /** effective = base + every patch the host has not acknowledged. */
  private rebase(): void {
    const base = this.base!;
    const pending = [...this.emitted, ...this.unflushed];
    this.effective = base;
    if (pending.length === 0) return;
    try {
      this.effective = applyPatches(base, pending);
    } catch {
      this.effective = base;
    }
  }

  private clearTimer(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }
}

/** True when `value` already shows the effect of `patch`. */
export function reflects(value: object, patch: Patch): boolean {
  // An append has no fixed path: it is reflected once the array ends with it.
  if (patch.op === "set" && patch.path.endsWith("/-")) {
    const list = getPointer(value, patch.path.slice(0, -2));
    return Array.isArray(list) && list.length > 0 && deepEqual(list[list.length - 1], patch.value);
  }
  const current = getPointer(value, patch.path);
  return patch.op === "set" ? deepEqual(current, patch.value) : current === undefined;
}
