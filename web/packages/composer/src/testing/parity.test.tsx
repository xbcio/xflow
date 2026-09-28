import { describe, expect, it } from "vitest";
import { jsonRenderKernel } from "../kernel-json-render";
import { nativeKernel } from "../kernel-native";
import { createConformanceRegistry } from "./components";
import { parityFixtures } from "./fixtures";
import { mountComposer } from "./harness";

describe("kernel parity: native vs json-render", () => {
  for (const fixture of parityFixtures()) {
    it(`renders identical DOM for ${fixture.name}`, async () => {
      const html = [];
      for (const kernel of [nativeKernel, jsonRenderKernel]) {
        const m = mountComposer({ kernel, registry: createConformanceRegistry(), ...fixture });
        await m.flush();
        html.push(m.container.innerHTML);
        expect(m.log).toEqual([]);
        m.unmount();
      }
      expect(html[0]).not.toBe("");
      expect(html[1]).toBe(html[0]);
    });
  }
});
