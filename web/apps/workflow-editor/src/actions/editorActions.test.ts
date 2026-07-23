import { describe, expect, it, vi } from "vitest";
import {
  publishWorkflow,
  redo,
  runWorkflow,
  saveWorkflow,
  undo,
  validateWorkflow,
} from "./editorActions";

describe("editorActions", () => {
  it("logs save workflow", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await saveWorkflow({ name: "wf" });
    expect(log).toHaveBeenCalledWith("[editor] saveWorkflow", "wf");
    log.mockRestore();
  });

  it("logs validate workflow", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await validateWorkflow({ name: "wf" });
    expect(log).toHaveBeenCalledWith("[editor] validateWorkflow", "wf");
    log.mockRestore();
  });

  it("logs publish workflow", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await publishWorkflow({ name: "wf" });
    expect(log).toHaveBeenCalledWith("[editor] publishWorkflow", "wf");
    log.mockRestore();
  });

  it("logs run workflow", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await runWorkflow({ name: "wf" });
    expect(log).toHaveBeenCalledWith("[editor] runWorkflow", "wf");
    log.mockRestore();
  });

  it("logs undo", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await undo();
    expect(log).toHaveBeenCalledWith("[editor] undo");
    log.mockRestore();
  });

  it("logs redo", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await redo();
    expect(log).toHaveBeenCalledWith("[editor] redo");
    log.mockRestore();
  });
});
