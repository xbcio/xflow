import { describe, expect, it } from "vitest";
import { nodeLibraryLabel, nodeLibraryMatches, nodePurposeByType, nodeTilePurpose } from "./nodeLibraryCopy";
import generated from "./node-form/testdata/node-types.generated.json";

const fixtureTypes = [...new Set((generated as { node_types: { node_type: string }[] }).node_types.map((item) => item.node_type))];
const builtinTypes = [...fixtureTypes, "xflow.supply.external", "xflow.supply.static", "xflow.supply.http"];

describe("node library copy", () => {
  it("drops the 触发器 suffix and keeps a readable timer name", () => {
    expect(nodeLibraryLabel("Webhook 触发器")).toBe("Webhook");
    expect(nodeLibraryLabel("Cron 触发器")).toBe("Cron");
    expect(nodeLibraryLabel("Kafka 触发器")).toBe("Kafka");
    expect(nodeLibraryLabel("Redis 触发器")).toBe("Redis");
    expect(nodeLibraryLabel("定时触发器")).toBe("定时器");
  });

  it("leaves names without the suffix alone", () => {
    expect(nodeLibraryLabel("HTTP 请求")).toBe("HTTP 请求");
    expect(nodeLibraryLabel("审批")).toBe("审批");
    expect(nodeLibraryLabel("  Webhook  ")).toBe("Webhook");
    expect(nodeLibraryLabel("", "acme.custom")).toBe("acme.custom");
  });

  it("has a short purpose for every builtin type", () => {
    for (const type of builtinTypes) {
      const purpose = nodePurposeByType[type];
      expect(purpose, `${type} has a purpose`).toBeTruthy();
      expect(purpose.length, `${type} stays on one line`).toBeLessThanOrEqual(16);
    }
  });

  it("prefers the descriptor description, then the table, then the type id", () => {
    expect(nodeTilePurpose({ type: "xflow.http", description: "  自定义说明  " })).toBe("自定义说明");
    expect(nodeTilePurpose({ type: "xflow.http" })).toBe("调用外部 API");
    expect(nodeTilePurpose({ type: "acme.unknown" })).toBe("acme.unknown");
  });

  it("matches a tile by label, type, or description", () => {
    const tile = { label: "HTTP", type: "xflow.http", description: "调用外部 HTTP 服务" };
    expect(nodeLibraryMatches(tile, "")).toBe(true);
    expect(nodeLibraryMatches(tile, "  ")).toBe(true);
    expect(nodeLibraryMatches(tile, "http")).toBe(true);
    expect(nodeLibraryMatches(tile, "XFLOW.HTTP")).toBe(true);
    expect(nodeLibraryMatches(tile, "服务")).toBe(true);
    expect(nodeLibraryMatches(tile, "调用外部")).toBe(true);
    expect(nodeLibraryMatches(tile, "no-such-node")).toBe(false);
  });

  it("keeps the description optional in the search predicate", () => {
    const tile = { label: "HTTP", type: "xflow.http" };
    expect(nodeLibraryMatches(tile, "http")).toBe(true);
    expect(nodeLibraryMatches(tile, "服务")).toBe(false);
    // A whitespace-only description is not a match for everything.
    expect(nodeLibraryMatches({ ...tile, description: "   " }, "服务")).toBe(false);
  });
});
