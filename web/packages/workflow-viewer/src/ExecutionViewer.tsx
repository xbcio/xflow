import * as React from "react";
import type { WorkflowDef } from "@xflow/workflow-core";
import type { ExecutionSnapshot } from "@xflow/workflow-renderer";
import { WorkflowViewer } from "./WorkflowViewer";

export interface ExecutionViewerProps {
  definition: WorkflowDef;
  executionSnapshot: ExecutionSnapshot;
  className?: string;
}

export function ExecutionViewer({
  definition,
  executionSnapshot,
  className,
}: ExecutionViewerProps) {
  const counts = React.useMemo(() => {
    const statuses = executionSnapshot.nodeStatuses ?? {};
    const result: Record<"running" | "completed" | "failed" | "suspended", number> = {
      running: 0,
      completed: 0,
      failed: 0,
      suspended: 0,
    };
    for (const status of Object.values(statuses)) {
      if (status && status in result) {
        result[status]++;
      }
    }
    return result;
  }, [executionSnapshot]);

  return (
    <div style={{ width: "100%", height: "100%", position: "relative" }}>
      <WorkflowViewer
        definition={definition}
        executionSnapshot={executionSnapshot}
        className={className}
      />
      <div
        style={{
          position: "absolute",
          top: 12,
          left: "50%",
          transform: "translateX(-50%)",
          zIndex: 10,
          background: "rgba(255, 255, 255, 0.95)",
          padding: "6px 12px",
          borderRadius: 999,
          boxShadow: "0 1px 4px rgba(0, 0, 0, 0.15)",
          fontSize: 12,
          fontWeight: 600,
          whiteSpace: "nowrap",
        }}
        data-testid="execution-summary"
      >
        Running: {counts.running} · Completed: {counts.completed} · Failed:{" "}
        {counts.failed} · Suspended: {counts.suspended}
      </div>
    </div>
  );
}
