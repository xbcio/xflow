// Test host around <Composer>: a controlled value applied with core
// applyPatches, a patch log and a warning log. Always renders in StrictMode.

import { act, render, type RenderResult } from "@testing-library/react";
import { StrictMode, useCallback, useState } from "react";
import { applyPatches, type ComposerSpec, type Patch, type Warning } from "../core";
import { Composer } from "../react/Composer";
import type { Registry, RenderKernel, ValidationResult } from "../react/contract";

export interface MountOptions {
  kernel: RenderKernel;
  registry: Registry;
  spec: ComposerSpec;
  value: object;
  context?: Record<string, unknown>;
  readOnly?: boolean;
  /** Delay (ms) before the host applies patches; 0 applies synchronously in onChange. */
  ackDelayMs?: number;
  /** When true the host ignores onChange entirely. */
  ignoreChanges?: boolean;
  onValidate?(result: ValidationResult): void;
}

export interface Mounted extends RenderResult {
  /** Every onChange call, in order. */
  log: Patch[][];
  warnings: Warning[];
  /** The host's current value. */
  readonly value: object;
  /** Replaces the host value (an external edit). */
  setValue(update: (value: object) => object): Promise<void>;
  /** Flushes microtasks (batched onChange) and resulting renders. */
  flush(): Promise<void>;
}

export function mountComposer(options: MountOptions): Mounted {
  const log: Patch[][] = [];
  const warnings: Warning[] = [];
  let current = options.value;
  let setHost: ((update: (value: object) => object) => void) | undefined;

  function Host() {
    const [value, setValue] = useState<object>(options.value);
    current = value;
    setHost = setValue;
    const onChange = useCallback((patches: Patch[]) => {
      log.push(patches);
      if (options.ignoreChanges) return;
      const apply = () => setValue((prev) => applyPatches(prev, patches));
      if (options.ackDelayMs) setTimeout(() => act(apply), options.ackDelayMs);
      else apply();
    }, []);
    const onWarning = useCallback((warning: Warning) => warnings.push(warning), []);
    return (
      <Composer
        spec={options.spec}
        registry={options.registry}
        kernel={options.kernel}
        value={value}
        context={options.context}
        readOnly={options.readOnly}
        onChange={onChange}
        onWarning={onWarning}
        onValidate={options.onValidate}
      />
    );
  }

  const utils = render(
    <StrictMode>
      <Host />
    </StrictMode>
  );
  const extra = {
    log,
    warnings,
    get value() {
      return current;
    },
    async setValue(update: (value: object) => object) {
      await act(async () => setHost!(update));
    },
    async flush() {
      await act(async () => {});
    }
  };
  // Object.assign would snapshot the `value` getter; copy descriptors instead.
  return Object.defineProperties(utils, Object.getOwnPropertyDescriptors(extra)) as Mounted;
}

/** Flattens the patch log. */
export function allPatches(log: Patch[][]): Patch[] {
  return log.flat();
}
