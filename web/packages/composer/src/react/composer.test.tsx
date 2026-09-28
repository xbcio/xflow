import { act, fireEvent, render } from "@testing-library/react";
import { StrictMode, useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { defineCheck, zodProps, type Patch, type PropsSchema, type Warning } from "../core";
import { jsonRenderKernel } from "../kernel-json-render";
import { nativeKernel } from "../kernel-native";
import { spec } from "../testing/fixtures";
import { mountComposer } from "../testing/harness";
import { Composer } from "./Composer";
import type { ComposerComponent, ComposerComponentProps, ValidationResult } from "./contract";
import { defaultKernel } from "./defaultKernel";
import { OVERLAY_TIMEOUT_MS } from "./overlay";
import { createRegistry } from "./registry";

const loose: PropsSchema<Record<string, unknown>> = { parse: (input) => ({ ok: true, value: input as Record<string, unknown> }) };

let lastProps: Record<string, ComposerComponentProps<unknown>> = {};

const Field: ComposerComponent<Record<string, unknown>> = {
  type: "Field",
  props: loose,
  render: (p) => {
    lastProps[p.node.id] = p as ComposerComponentProps<unknown>;
    return (
      <input
        data-node={p.node.id}
        value={typeof p.value === "string" ? p.value : ""}
        placeholder={p.defaultHint === undefined ? "" : `default: ${String(p.defaultHint)}`}
        readOnly={p.readOnly}
        onChange={(event) => p.onChange?.(event.target.value)}
      />
    );
  }
};
const KeepField: ComposerComponent<Record<string, unknown>> = { ...Field, type: "KeepField", emptyAs: "keep" };
const Group: ComposerComponent<Record<string, unknown>> = {
  type: "Group",
  props: loose,
  bindings: { value: "object" },
  render: ({ node, children }) => <fieldset data-node={node.id}>{children}</fieldset>
};
const Range: ComposerComponent<Record<string, unknown>> = {
  type: "Range",
  props: loose,
  render: (p) => {
    lastProps[p.node.id] = p as ComposerComponentProps<unknown>;
    return <span data-node={p.node.id}>{`${String(p.bindings.min?.value)}-${String(p.bindings.max?.value)}`}</span>;
  }
};
const Typed: ComposerComponent<{ label: string }> = {
  type: "Typed",
  props: zodProps(z.object({ label: z.string() })),
  render: ({ props }) => <span className="typed">{props.label}</span>
};

const registry = createRegistry([Field, KeepField, Group, Range, Typed], { debug: () => {} });
const byNode = (container: HTMLElement, id: string) => container.querySelector<HTMLInputElement>(`[data-node="${id}"]`)!;

afterEach(() => {
  lastProps = {};
  vi.useRealTimers();
});

describe("createRegistry", () => {
  it("rejects duplicate types within one call", () => {
    expect(() => createRegistry([Field, Field])).toThrow(/duplicate component type Field/);
    expect(() => createRegistry([{ ...Field, type: "$row" }])).toThrow(/reserved/);
  });

  it("lets later registrations override earlier ones, with a debug log", () => {
    const debug = vi.fn();
    const override = { ...Field, render: () => <b>override</b> };
    const next = createRegistry([override], { extends: registry, debug });
    expect(next.get("Field")).toBe(override);
    expect(next.get("Group")).toBe(Group);
    expect(debug).toHaveBeenCalledWith(expect.stringMatching(/Field overrides/));
    expect(next.bindingKinds.Group).toEqual({ value: "object" });
    expect(next.bindingKinds.Field).toEqual({ value: "scalar" });
  });

  it("merges checks and expressions, overriding builtins with a debug log", () => {
    const debug = vi.fn();
    const custom = defineCheck("even", (value) => (typeof value === "number" && value % 2 === 1 ? [{}] : []));
    const r = createRegistry([], { checks: [custom, defineCheck("required", () => [])], debug });
    expect(r.checks.get("even")).toBe(custom);
    expect(r.checks.get("min")).toBeDefined();
    expect(debug).toHaveBeenCalledWith(expect.stringMatching(/check required overrides/));
    expect(() => createRegistry([], { checks: [custom, custom] })).toThrow(/duplicate check/);
  });
});

describe("<Composer>", () => {
  it("defaults to the json-render kernel", () => {
    expect(defaultKernel).toBe(jsonRenderKernel);
    const { container } = render(
      <Composer spec={spec({ root: { type: "Typed", props: { label: "hello" } } })} registry={registry} value={{}} onChange={() => {}} />
    );
    expect(container.querySelector(".typed")?.textContent).toBe("hello");
  });

  it("passes value/onChange shorthands, every binding, defaultHint, issues and readOnly", async () => {
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({
        root: { type: "Group", children: ["name", "range"] },
        name: {
          type: "Field",
          props: { value: { $bindState: "/name" }, defaultHint: "anon" },
          checks: [{ type: "required", message: "required!" }]
        },
        range: { type: "Range", props: { min: { $bindState: "/min" }, max: { $bindState: "/max" } } }
      }),
      value: { min: 1, max: 5 }
    });
    await m.flush();
    const name = lastProps.name;
    expect(name.value).toBeUndefined();
    expect(name.onChange).toBe(name.bindings.value.onChange);
    expect(name.defaultHint).toBe("anon");
    expect(name.issues.map((issue) => issue.message)).toEqual(["required!"]);
    expect(name.readOnly).toBe(false);
    expect(byNode(m.container, "name").placeholder).toBe("default: anon");
    expect(byNode(m.container, "range").textContent).toBe("1-5");

    await act(async () => lastProps.range.bindings.max.onChange(9));
    expect(m.log).toEqual([[{ op: "set", path: "/max", value: 9 }]]);
    expect(byNode(m.container, "range").textContent).toBe("1-9");
  });

  it("merges writes of one microtask into one onChange, last write per path wins", async () => {
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({
        root: { type: "Group", children: ["a", "b"] },
        a: { type: "Field", props: { value: { $bindState: "/a" } } },
        b: { type: "Field", props: { value: { $bindState: "/b" } } }
      }),
      value: {}
    });
    await m.flush();
    await act(async () => {
      lastProps.a.onChange!("1");
      lastProps.b.onChange!("x");
      lastProps.a.onChange!("2");
    });
    expect(m.log).toEqual([
      [
        { op: "set", path: "/b", value: "x" },
        { op: "set", path: "/a", value: "2" }
      ]
    ]);
    expect(m.value).toEqual({ a: "2", b: "x" });
  });

  it("clears to unset (collapsing object-bound parents) and honours emptyAs: keep", async () => {
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({
        root: { type: "Group", children: ["headers", "keep"] },
        headers: { type: "Group", props: { value: { $bindState: "/p/headers" } }, children: ["h"] },
        h: { type: "Field", props: { value: { $bindState: "/p/headers/h" } } },
        keep: { type: "KeepField", props: { value: { $bindState: "/p/keep" } } }
      }),
      value: { p: { headers: { h: "v" }, keep: "k" } }
    });
    await m.flush();
    fireEvent.change(byNode(m.container, "h"), { target: { value: "" } });
    fireEvent.change(byNode(m.container, "keep"), { target: { value: "" } });
    await m.flush();
    expect(m.log.flat()).toEqual([
      { op: "unset", path: "/p/headers" },
      { op: "set", path: "/p/keep", value: "" }
    ]);
    expect(m.value).toEqual({ p: { keep: "" } });
  });

  it("drops the overlay when the host passes a value without the patches (rejection)", async () => {
    let setHost: (value: object) => void = () => {};
    const onChange = vi.fn();
    function Host() {
      const [value, setValue] = useState<object>({ name: "a" });
      setHost = setValue;
      return (
        <Composer
          kernel={nativeKernel}
          registry={registry}
          spec={spec({ root: { type: "Field", props: { value: { $bindState: "/name" } } } })}
          value={value}
          onChange={onChange}
        />
      );
    }
    const { container } = render(
      <StrictMode>
        <Host />
      </StrictMode>
    );
    const input = byNode(container, "root");
    fireEvent.change(input, { target: { value: "ab" } });
    expect(input.value).toBe("ab"); // overlay
    await act(async () => {});
    expect(onChange).toHaveBeenCalledWith([{ op: "set", path: "/name", value: "ab" }]);
    expect(input.value).toBe("ab"); // still the overlay: host has not answered
    await act(async () => setHost({ name: "rejected" }));
    expect(input.value).toBe("rejected");
  });

  it("drops the overlay with a warning after the timeout when the host ignores onChange", async () => {
    vi.useFakeTimers();
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({ root: { type: "Field", props: { value: { $bindState: "/name" } } } }),
      value: { name: "a" },
      ignoreChanges: true
    });
    await m.flush();
    const input = byNode(m.container, "root");
    fireEvent.change(input, { target: { value: "ab" } });
    await m.flush();
    expect(m.log).toHaveLength(1);
    await act(async () => vi.advanceTimersByTime(OVERLAY_TIMEOUT_MS - 10));
    expect(input.value).toBe("ab");
    await act(async () => vi.advanceTimersByTime(20));
    expect(input.value).toBe("a");
    expect(m.warnings.map((w) => w.code)).toEqual(["overlay-timeout"]);
  });

  it("bases rapid successive writes on the overlay, not the stale host value", async () => {
    const Adder: ComposerComponent<Record<string, unknown>> = {
      type: "Adder",
      props: loose,
      bindings: { value: "array" },
      render: ({ value, onChange }) => (
        <button type="button" onClick={() => onChange?.([...((value as unknown[]) ?? []), { n: ((value as unknown[]) ?? []).length }])}>
          add
        </button>
      )
    };
    const m = mountComposer({
      kernel: nativeKernel,
      registry: createRegistry([Adder]),
      spec: spec({ root: { type: "Adder", props: { value: { $bindState: "/list" } } } }),
      value: {},
      ackDelayMs: 20
    });
    await m.flush();
    const button = m.container.querySelector("button")!;
    fireEvent.click(button);
    fireEvent.click(button);
    await m.flush();
    expect(m.log).toEqual([[{ op: "set", path: "/list", value: [{ n: 0 }, { n: 1 }] }]]);
  });

  it("rejects writes to /$ctx and exposes context read-only", async () => {
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({ root: { type: "Field", props: { value: { $bindState: "/$ctx/user" } } } }),
      value: {},
      context: { user: "ctx-user" }
    });
    await m.flush();
    const input = byNode(m.container, "root");
    expect(input.value).toBe("ctx-user");
    fireEvent.change(input, { target: { value: "x" } });
    await m.flush();
    expect(m.log).toEqual([]);
    expect(m.warnings.map((w) => w.code)).toEqual(["ctx-readonly"]);
  });

  it("readOnly passes readOnly and ignores writes", async () => {
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({ root: { type: "Field", props: { value: { $bindState: "/name" } } } }),
      value: { name: "a" },
      readOnly: true
    });
    await m.flush();
    expect(lastProps.root.readOnly).toBe(true);
    await act(async () => lastProps.root.onChange!("b"));
    expect(m.log).toEqual([]);
    expect(m.warnings.map((w) => w.code)).toEqual(["read-only"]);
  });

  it("emit only warns in v1", async () => {
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({ root: { type: "Field", props: {} } }),
      value: {}
    });
    await m.flush();
    lastProps.root.emit("press", { x: 1 });
    expect(m.warnings.map((w) => w.code)).toEqual(["emit-reserved"]);
    expect(m.log).toEqual([]);
  });

  it("reports validation (errors, warnings, external issues) through onValidate", async () => {
    const results: ValidationResult[] = [];
    const m = mountComposer({
      kernel: nativeKernel,
      registry,
      spec: spec({
        root: { type: "Group", children: ["a"] },
        a: {
          type: "Field",
          props: { value: { $bindState: "/a" } },
          checks: [
            { type: "required", message: "a required" },
            { type: "minLength", args: { value: 3 }, message: "short", severity: "warning" }
          ]
        }
      }),
      value: {},
      onValidate: (result) => results.push(result)
    });
    await m.flush();
    expect(results.at(-1)?.valid).toBe(false);
    expect(results.at(-1)?.errors.map((issue) => issue.message)).toEqual(["a required"]);
    fireEvent.change(byNode(m.container, "a"), { target: { value: "x" } });
    await m.flush();
    const last = results.at(-1)!;
    expect(last.valid).toBe(true);
    expect(last.warnings.map((issue) => issue.message)).toEqual(["short"]);
    const count = results.length;
    fireEvent.change(byNode(m.container, "a"), { target: { value: "y" } });
    await m.flush();
    expect(results.length).toBe(count); // unchanged result is not re-reported
  });

  it("merges externalIssues into node issues", () => {
    render(
      <Composer
        kernel={nativeKernel}
        registry={registry}
        spec={spec({ root: { type: "Field", props: { value: { $bindState: "/a" } } } })}
        value={{}}
        externalIssues={{ "/a": [{ message: "server says no", severity: "error" }] }}
        onChange={() => {}}
      />
    );
    expect(lastProps.root.issues.map((issue) => issue.message)).toEqual(["server says no"]);
  });

  it("renders props-schema failures as ElementError and uses registered placeholders", () => {
    const warnings: Warning[] = [];
    const Unsupported: ComposerComponent<{ message: string; type: string }> = {
      type: "Unsupported",
      props: loose as PropsSchema<{ message: string; type: string }>,
      render: ({ props }) => <em className="custom-unsupported">{props.type}</em>
    };
    const { container } = render(
      <Composer
        kernel={jsonRenderKernel}
        registry={createRegistry([Unsupported], { extends: registry, debug: () => {} })}
        spec={spec({
          root: { type: "Group", children: ["bad", "unknown"] },
          bad: { type: "Typed", props: { label: 42 } },
          unknown: { type: "Missing" }
        })}
        value={{}}
        onChange={(_patches: Patch[]) => {}}
        onWarning={(w) => warnings.push(w)}
      />
    );
    expect(container.querySelector('[data-composer-placeholder="error"]')?.getAttribute("data-composer-node")).toBe("bad");
    expect(container.querySelector(".custom-unsupported")?.textContent).toBe("Missing");
  });
});
