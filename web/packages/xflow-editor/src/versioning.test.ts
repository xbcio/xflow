import { describe, expect, it } from "vitest";
import {
  bareVersion,
  publishErrorText,
  rollbackErrorText,
  suggestFeatureVersion,
  suggestNextVersion,
  versionHashPrefix,
  versionLabel,
  versionUnavailableReason
} from "./versioning";

const apiError = (status: number, code: string, message = "server text") => ({ status, code, message });

describe("suggestNextVersion", () => {
  it("bumps the patch of a semver label", () => {
    expect(suggestNextVersion("1.2.0")).toBe("1.2.1");
    expect(suggestNextVersion("v1.2.9")).toBe("1.2.10");
  });

  it("starts an unpublished workflow at 0.1.0", () => {
    expect(suggestNextVersion(undefined)).toBe("0.1.0");
    expect(suggestNextVersion("  ")).toBe("0.1.0");
  });

  it("asks for a label instead of inventing one when the version is not semver", () => {
    expect(suggestNextVersion("2026-10-19")).toBe("");
    expect(suggestNextVersion("release-a")).toBe("");
  });

  it("offers the feature bump next to the patch one", () => {
    expect(suggestFeatureVersion("1.2.0")).toBe("1.3.0");
    expect(suggestFeatureVersion("v1.2.9")).toBe("1.3.0");
    expect(suggestFeatureVersion(undefined)).toBe("");
    expect(suggestFeatureVersion("release-a")).toBe("");
  });
});

describe("version labels", () => {
  it("prefixes v unless the label already carries it", () => {
    expect(versionLabel("1.2.0")).toBe("v1.2.0");
    expect(versionLabel("v1.2.0")).toBe("v1.2.0");
    expect(bareVersion("v1.2.0")).toBe("1.2.0");
    expect(bareVersion("1.2.0")).toBe("1.2.0");
  });

  it("shortens a hash to its leading bytes and leaves a short hash whole", () => {
    expect(versionHashPrefix("c07d1234abcd")).toBe("c07d1234…");
    expect(versionHashPrefix("c07d19")).toBe("c07d19");
    expect(versionHashPrefix(undefined)).toBe("hash 不可用");
  });
});

describe("publishErrorText", () => {
  it("maps the three publish conflicts to their own sentences", () => {
    expect(publishErrorText(apiError(409, "workflow_version_immutable"))).toContain("换一个新版本号");
    expect(publishErrorText(apiError(409, "workflow_name_conflict"))).toContain("改名");
    expect(publishErrorText(apiError(409, "workflow_conflict"))).toContain("重新加载最新定义");
  });

  it("falls back to the server message, then to 发布失败", () => {
    expect(publishErrorText(apiError(400, "workflow_invalid", "spec is missing"))).toBe("spec is missing");
    expect(publishErrorText(new Error("network down"))).toBe("network down");
    expect(publishErrorText(undefined)).toBe("发布失败");
    expect(publishErrorText({ message: "  " })).toBe("发布失败");
  });
});

describe("rollback errors", () => {
  it("pins only workflow_version_unavailable to the row", () => {
    const reason = versionUnavailableReason(apiError(409, "workflow_version_unavailable"));
    expect(reason).toContain("哈希不一致");
    expect(versionUnavailableReason(apiError(409, "workflow_conflict"))).toBeUndefined();
    expect(versionUnavailableReason(new Error("boom"))).toBeUndefined();
  });

  it("explains a version that fell out of retention", () => {
    expect(rollbackErrorText(apiError(404, "workflow_version_not_found"))).toContain("保留期");
    expect(rollbackErrorText(apiError(500, "internal_error", "boom"))).toBe("boom");
    expect(rollbackErrorText(undefined)).toBe("回滚失败");
  });
});
