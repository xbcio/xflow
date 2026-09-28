// Shared specs for the conformance suite and the kernel parity check.

import type { ComposerSpec, ElementDef } from "../core";

export function spec(elements: Record<string, ElementDef>, root = "root"): ComposerSpec {
  return { spec: "composer/v1", minor: 0, root, elements };
}

export interface ConformanceFixture {
  name: string;
  spec: ComposerSpec;
  value: object;
  context?: Record<string, unknown>;
}

export const treeSpec = spec({
  root: { type: "Box", children: ["t1", "inner", "t4"] },
  t1: { type: "Text", props: { text: "one" } },
  inner: { type: "Box", children: ["t2", "t3"] },
  t2: { type: "Text", props: { text: "two" } },
  t3: { type: "Text", props: { text: { $state: "/three" } } },
  t4: { type: "Text", props: { text: { $state: "/$ctx/four" } } }
});

export const slotSpec = spec({
  root: { type: "Box", children: ["body"], slots: { header: ["h1", "h2"], footer: ["f1"] } },
  body: { type: "Text", props: { text: "body" } },
  h1: { type: "Text", props: { text: "head-1" } },
  h2: { type: "Text", props: { text: "head-2" } },
  f1: { type: "Text", props: { text: "foot" } }
});

export const rowsSpec = (input: "Input" | "LocalInput" = "LocalInput", key?: string) =>
  spec({
    root: { type: "Box", children: ["list"] },
    list: {
      type: "List",
      props: { value: { $bindState: "/rows" } },
      repeat: key === undefined ? { statePath: "/rows" } : { statePath: "/rows", key },
      children: ["label", "name"]
    },
    label: { type: "Text", props: { text: { $item: "name" } } },
    name: { type: input, props: { value: { $bindItem: "name" } } }
  });

export const fieldsSpec = spec({
  root: { type: "Box", children: ["f0", "f1", "f2", "grp", "t"] },
  f0: { type: "Input", props: { value: { $bindState: "/f0" } } },
  f1: { type: "Input", props: { value: { $bindState: "/f1" }, defaultHint: "d1" } },
  f2: { type: "Input", props: { value: { $bindState: "/f2" } } },
  grp: { type: "Box", children: ["f3", "f4"] },
  f3: { type: "Input", props: { value: { $bindState: "/g/f3" } } },
  f4: { type: "Input", props: { value: { $bindState: "/g/f4" } } },
  t: { type: "Text", props: { text: "static" } }
});

export const toggleSpec = spec({
  root: { type: "Box", children: ["mode", "a", "b"] },
  mode: { type: "Toggle", props: { value: { $bindState: "/mode" }, options: ["a", "b"] } },
  a: { type: "Input", props: { value: { $bindState: "/x" } }, visible: { $state: "/mode", eq: "a" } },
  b: { type: "Input", props: { value: { $bindState: "/x" } }, visible: { $state: "/mode", eq: "b" } }
});

export const errorSpec = spec({
  root: { type: "Box", children: ["before", "unknown", "boom", "after"] },
  before: { type: "Text", props: { text: "before" } },
  unknown: { type: "Nope", props: { label: "x" } },
  boom: { type: "Boom", props: { fail: { $state: "/fail" } } },
  after: { type: "Text", props: { text: "after" } }
});

const rows = () => [{ name: "a" }, { name: "b" }, { name: "c" }];

/** Specs rendered by both kernels for the DOM parity check. */
export function parityFixtures(): ConformanceFixture[] {
  return [
    { name: "tree", spec: treeSpec, value: { three: "three" }, context: { four: "four" } },
    { name: "slots", spec: slotSpec, value: {} },
    { name: "rows (object identity)", spec: rowsSpec("Input"), value: { rows: rows() } },
    { name: "rows (keyed)", spec: rowsSpec("LocalInput", "name"), value: { rows: rows() } },
    { name: "empty rows", spec: rowsSpec("Input"), value: {} },
    { name: "fields", spec: fieldsSpec, value: { f0: "zero", g: { f3: "three" } } },
    { name: "visible", spec: toggleSpec, value: { mode: "b", x: "shared" } },
    { name: "placeholders", spec: errorSpec, value: { fail: false } },
    {
      name: "nested repeat",
      spec: spec({
        root: { type: "List", props: { value: { $bindState: "/groups" } }, repeat: { statePath: "/groups" }, children: ["title", "items"] },
        title: { type: "Text", props: { text: { $item: "title" } } },
        items: { type: "List", props: { value: { $bindItem: "items" } }, repeat: { statePath: { $item: "items" } }, children: ["item"] },
        item: { type: "Text", props: { text: { $item: "" } } }
      }),
      value: { groups: [{ title: "g1", items: ["x", "y"] }, { title: "g2", items: [] }] }
    }
  ];
}
