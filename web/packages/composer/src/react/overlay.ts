// Overlay and microtask batching (Doc B §6 rule 6).
//
// - Writes inside one microtask are merged (same path: last wins, via core
//   createPatchBatch/mergePatches) into a single onChange call.
// - The overlay is every patch written since the host last passed a new
//   `value` reference. Until then resolve and later writes see
//   "host value + overlay", so rapid edits never compute from stale data.
// - A new host `value` reference drops the overlay: the host value wins
//   (patches it rejected disappear). Patches not yet flushed are re-applied
//   on top of the new host value, since the host has not seen them.
// - If the host has not passed a new value within OVERLAY_TIMEOUT_MS of the
//   first unacknowledged flush, the overlay is dropped with a warning.

import { applyPatches, createPatchBatch, write, type BindTarget, type Patch, type ResolvedTree, type ValueKind, type Warning } from "../core";

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
  private emitted = false;
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
      this.emitted = false;
      this.clearTimer();
      this.effective = hostValue;
      if (this.unflushed.length > 0) {
        try {
          this.effective = applyPatches(hostValue, this.unflushed);
        } catch {
          this.effective = hostValue;
        }
      }
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
    this.emitted = true;
    if (this.active && this.timer === undefined) {
      this.timer = setTimeout(() => this.expire(), OVERLAY_TIMEOUT_MS);
    }
    this.host.onChange(patches);
  }

  /** Re-enables the timeout/invalidation after a (StrictMode) remount. */
  activate(): void {
    this.active = true;
    if (this.emitted && this.timer === undefined) {
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
    if (!this.emitted || this.base === undefined) return;
    this.emitted = false;
    this.host.onWarning({
      code: "overlay-timeout",
      message: `host did not pass a new value within ${OVERLAY_TIMEOUT_MS}ms of onChange; dropping the local overlay`
    });
    this.effective = this.base;
    if (this.unflushed.length > 0) {
      try {
        this.effective = applyPatches(this.base, this.unflushed);
      } catch {
        this.effective = this.base;
      }
    }
    if (this.active) this.host.invalidate();
  }

  private clearTimer(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }
}
