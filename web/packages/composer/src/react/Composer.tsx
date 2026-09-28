// <Composer> (Doc B §6): wires core resolve/write to a kernel and owns the
// overlay, microtask batching and the node renderer.

import { useEffect, useMemo, useReducer, useRef } from "react";
import { deepEqual, resolve, type ResolvedTree, type Warning } from "../core";
import type { ComposerProps, ValidationResult } from "./contract";
import { defaultKernel } from "./defaultKernel";
import { OverlayController, type OverlayHost } from "./overlay";
import { createNodeRenderer } from "./renderNode";

const defaultWarn = (warning: Warning) => console.warn(`[composer] ${warning.code}: ${warning.message}`);

export function Composer(props: ComposerProps) {
  const { spec, registry, context, externalIssues, onValidate } = props;
  const kernel = props.kernel ?? defaultKernel;
  const readOnly = props.readOnly ?? false;
  const [, invalidate] = useReducer((n: number) => n + 1, 0);

  // Latest callbacks, readable from the controller and stable closures.
  const hostRef = useRef<ComposerProps>(props);
  hostRef.current = props;
  const overlayHost = useMemo<OverlayHost>(
    () => ({
      onChange: (patches) => hostRef.current.onChange(patches),
      onWarning: (warning) => (hostRef.current.onWarning ?? defaultWarn)(warning),
      invalidate
    }),
    []
  );
  const controllerRef = useRef<OverlayController | null>(null);
  controllerRef.current ??= new OverlayController(overlayHost);
  const controller = controllerRef.current;

  useEffect(() => {
    controller.activate();
    return () => controller.deactivate();
  }, [controller]);

  const value = controller.sync(props.value);

  const previousRef = useRef<ResolvedTree | undefined>(undefined);
  const tree = useMemo(
    () =>
      resolve({
        spec,
        value,
        context,
        externalIssues,
        schemas: registry.schemas,
        bindingKinds: registry.bindingKinds,
        checks: registry.checks,
        expressions: registry.expressions,
        previous: previousRef.current
      }),
    [spec, value, context, externalIssues, registry]
  );
  previousRef.current = tree;
  controller.tree = tree;
  const treeRef = useRef(tree);
  treeRef.current = tree;

  const env = useMemo(
    () => ({
      renderNode: createNodeRenderer({
        registry,
        readOnly,
        getTree: () => treeRef.current,
        write: (target, next, valueKind, emptyAs) => controller.write(target, next, valueKind, emptyAs),
        warn: (warning) => overlayHost.onWarning(warning)
      })
    }),
    [registry, readOnly, controller, overlayHost]
  );

  useValidationReport(tree, onValidate);

  return <>{kernel.render(tree, env)}</>;
}

function useValidationReport(tree: ResolvedTree, onValidate: ComposerProps["onValidate"]): void {
  const lastRef = useRef<ValidationResult | null>(null);
  useEffect(() => {
    if (!onValidate) return;
    const result = validationResult(tree);
    if (lastRef.current && deepEqual(lastRef.current, result)) return;
    lastRef.current = result;
    onValidate(result);
  }, [tree, onValidate]);
}

export function validationResult(tree: ResolvedTree): ValidationResult {
  const errors = tree.issues.filter((issue) => issue.severity === "error");
  const warnings = tree.issues.filter((issue) => issue.severity === "warning");
  const specErrors = tree.specErrors;
  return {
    issues: tree.issues,
    errors,
    warnings,
    specErrors,
    valid: errors.length === 0 && !specErrors.some((error) => error.severity === "error")
  };
}
