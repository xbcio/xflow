// Minimal test-only components for the conformance suite (no AntD; the real
// form components are composer/form). They follow the component contract
// strictly: they never write on mount.

import { useEffect, useRef, useState } from "react";
import { z } from "zod";
import { zodProps, type PropsSchema } from "../core";
import type { ComposerComponent, ComposerComponentProps } from "../react/contract";
import { createRegistry } from "../react/registry";

/** Records which elements rendered, so tests can assert re-render scope. */
export interface RenderProbe {
  /** Node ids in render order (StrictMode renders twice; compare as sets). */
  renders: string[];
  reset(): void;
}

export function createProbe(): RenderProbe {
  const probe: RenderProbe = {
    renders: [],
    reset() {
      probe.renders.length = 0;
    }
  };
  return probe;
}

const loose: PropsSchema<Record<string, unknown>> = {
  parse: (input) => ({ ok: true, value: (input ?? {}) as Record<string, unknown> })
};

const textProps = zodProps(z.object({ text: z.unknown().optional() }));

function show(value: unknown): string {
  if (value === undefined || value === null) return "";
  return typeof value === "string" ? value : JSON.stringify(value);
}

/** Registry of the conformance components, recording renders into `probe`. */
export function createConformanceRegistry(probe: RenderProbe = createProbe()) {
  const Box: ComposerComponent<Record<string, unknown>> = {
    type: "Box",
    props: loose,
    render: ({ node, children, slots }) => (
      <div className="box" data-node={node.id}>
        {slots.header !== undefined && <div data-slot="header">{slots.header}</div>}
        {children}
        {slots.footer !== undefined && <div data-slot="footer">{slots.footer}</div>}
      </div>
    )
  };

  const Text: ComposerComponent<{ text?: unknown }> = {
    type: "Text",
    props: textProps,
    render: ({ node, props }) => {
      probe.renders.push(node.id);
      return (
        <span className="text" data-node={node.id}>
          {show(props.text)}
        </span>
      );
    }
  };

  const Input: ComposerComponent<Record<string, unknown>> = {
    type: "Input",
    props: loose,
    render: (p) => <ControlledInput {...p} probe={probe} />
  };

  const LocalInput: ComposerComponent<Record<string, unknown>> = {
    type: "LocalInput",
    props: loose,
    render: (p) => <DraftInput {...p} probe={probe} />
  };

  const List: ComposerComponent<Record<string, unknown>> = {
    type: "List",
    props: loose,
    bindings: { value: "array" },
    render: ({ node, rows }) => (
      <ul className="list" data-node={node.id}>
        {(rows ?? []).map((row) => (
          <li key={row.id} data-row={row.id} data-index={row.index}>
            {row.children}
          </li>
        ))}
      </ul>
    )
  };

  const Toggle: ComposerComponent<{ options?: unknown[] }> = {
    type: "Toggle",
    props: loose as PropsSchema<{ options?: unknown[] }>,
    render: ({ node, props, value, onChange, readOnly }) => {
      const options = props.options ?? [true, false];
      const next = options[(options.indexOf(value) + 1) % options.length];
      return (
        <button type="button" data-node={node.id} disabled={readOnly} onClick={() => onChange?.(next)}>
          {show(value)}
        </button>
      );
    }
  };

  const Boom: ComposerComponent<{ fail?: unknown }> = {
    type: "Boom",
    props: loose as PropsSchema<{ fail?: unknown }>,
    render: ({ node, props }) => {
      if (props.fail) throw new Error(`boom in ${node.id}`);
      return (
        <span className="boom" data-node={node.id}>
          ok
        </span>
      );
    }
  };

  return createRegistry([Box, Text, Input, LocalInput, List, Toggle, Boom]);
}

type ProbeProps = ComposerComponentProps<Record<string, unknown>> & { probe: RenderProbe };

/** Controlled input: every change is written immediately. */
function ControlledInput({ node, value, onChange, defaultHint, readOnly, probe }: ProbeProps) {
  probe.renders.push(node.id);
  return (
    <input
      data-node={node.id}
      value={show(value)}
      placeholder={defaultHint === undefined ? undefined : `default: ${show(defaultHint)}`}
      readOnly={readOnly}
      onChange={(event) => onChange?.(event.target.value)}
    />
  );
}

/**
 * Keeps a local draft and commits it on blur — and, like many real inputs,
 * on unmount when the draft is dirty (the case Doc B §3.4 guards against).
 */
function DraftInput({ node, value, onChange, probe }: ProbeProps) {
  probe.renders.push(node.id);
  const [draft, setDraft] = useState<string | null>(null);
  const pending = useRef<{ draft: string | null; onChange?: (next: unknown) => void }>({ draft: null });
  pending.current = { draft, onChange };
  useEffect(
    () => () => {
      const { draft: dirty, onChange: commit } = pending.current;
      if (dirty !== null) commit?.(dirty);
    },
    []
  );
  const commit = () => {
    if (draft !== null) onChange?.(draft);
    setDraft(null);
  };
  return (
    <input data-node={node.id} value={draft ?? show(value)} onChange={(event) => setDraft(event.target.value)} onBlur={commit} />
  );
}
