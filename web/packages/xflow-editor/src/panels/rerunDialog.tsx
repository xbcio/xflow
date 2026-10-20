import { Button, Checkbox, Modal, Select } from "antd";
import { workflowStatusMeta, type ExecutionStatusKey } from "@xflow/core";
import * as React from "react";

import type { XFlowExecution } from "./runBar";

/**
 * A rerun request (§6.9). The editor never calls the API: the host receives
 * this, owns the POST /v1/executions/{id}/rerun call and decides what to do
 * with the result (select the new execution, toast a queue position, …).
 */
export interface XFlowRerunRequest {
  executionId: string;
  /** Where the new run starts; upstream results are copied from the source. */
  fromNode: string;
  /** Required confirmation when the source execution succeeded. */
  includeSuccess: boolean;
  /** One key per dialog open: a retry reuses it, so a double submit is one rerun. */
  idempotencyKey: string;
}

/** The structural slice of `@xflow/api`'s `RerunExecutionResult` the dialog reads. */
export interface XFlowRerunResult {
  executionId: string;
  status?: ExecutionStatusKey;
  queuePosition?: number;
  duplicate?: boolean;
}

export interface XFlowRerunDialogProps {
  open: boolean;
  /** The source execution; its nodes are the starting-point choices. */
  execution?: XFlowExecution;
  /** The node the 属性面板 entry opened with; the default is the first failure. */
  initialNode?: string;
  getContainer?: () => HTMLElement;
  onCancel: () => void;
  /** Omit and the submit button stays disabled (the host owns the API call). */
  onSubmit?: (request: XFlowRerunRequest) => Promise<XFlowRerunResult> | XFlowRerunResult;
}

/** The §6.9 default: the first failed node, else the first node (从头). */
export function defaultRerunNode(nodes: { name: string; status: string }[] | undefined): string | undefined {
  if (!nodes || nodes.length === 0) return undefined;
  return nodes.find((node) => node.status === "failed")?.name ?? nodes[0].name;
}

/**
 * §6.9 rerun dialog, shared by the run bar and the Inspector's footer. It
 * restates no status table: the entry points only open it when
 * `workflowStatusMeta[status].ops` lists `rerun`, and the success confirmation
 * is the spec's `include_success` checkbox.
 */
export function RerunDialog({
  open,
  execution,
  initialNode,
  getContainer,
  onCancel,
  onSubmit
}: XFlowRerunDialogProps): React.ReactElement {
  const nodes = execution?.nodes ?? [];
  const [fromNode, setFromNode] = React.useState<string>();
  const [includeSuccess, setIncludeSuccess] = React.useState(false);
  const [submitting, setSubmitting] = React.useState(false);
  const [error, setError] = React.useState<string>();
  const [idempotencyKey, setIdempotencyKey] = React.useState("");
  // The host replaces `execution` on every 2s poll; a ref keeps the open-time
  // default (first failure) stable instead of resetting the choice each poll.
  const nodesRef = React.useRef(nodes);
  nodesRef.current = nodes;
  // The dialog is destroyed on close; this effect also covers an id change
  // between renders (a new source execution) without carrying the old choice.
  React.useEffect(() => {
    if (!open) return;
    const openNodes = nodesRef.current;
    const preferred = initialNode && openNodes.some((node) => node.name === initialNode)
      ? initialNode
      : defaultRerunNode(openNodes);
    setFromNode(preferred);
    setIncludeSuccess(false);
    setSubmitting(false);
    setError(undefined);
    setIdempotencyKey(
      typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
        ? crypto.randomUUID()
        : `rerun-${Date.now()}-${Math.random().toString(16).slice(2)}`
    );
  }, [open, execution?.executionId, initialNode]);

  const successSource = execution?.status === "success";
  const rerunnable = execution ? workflowStatusMeta[execution.status].ops.includes("rerun") : false;
  const canSubmit = Boolean(
    open && rerunnable && fromNode && idempotencyKey && onSubmit && (!successSource || includeSuccess) && !submitting
  );

  const submit = async () => {
    if (!execution || !fromNode || !onSubmit || !canSubmit) return;
    setSubmitting(true);
    setError(undefined);
    try {
      await onSubmit({ executionId: execution.executionId, fromNode, includeSuccess: successSource, idempotencyKey });
      onCancel();
    } catch (reason) {
      setError(reason instanceof Error && reason.message ? reason.message : "重跑提交失败，请稍后重试。");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      centered={false}
      className="xflow-editor-rerun"
      classNames={{
        body: "xflow-editor-rerun__body",
        container: "xflow-editor-rerun__content",
        footer: "xflow-editor-rerun__footer",
        header: "xflow-editor-rerun__header"
      }}
      destroyOnHidden
      footer={
        <div className="xflow-editor-rerun__actions">
          <Button onClick={onCancel}>取消</Button>
          <Button
            disabled={!canSubmit}
            loading={submitting}
            type="primary"
            onClick={() => void submit()}
          >
            从这里重跑
          </Button>
        </div>
      }
      getContainer={getContainer}
      open={open}
      title="重跑执行"
      onCancel={onCancel}
    >
      <p className="xflow-editor-rerun__sub">
        <span className="xflow-editor-rerun__id">{execution?.executionId}</span>
      </p>
      <label className="xflow-editor-rerun__field">
        <span>起点节点</span>
        <Select
          aria-label="起点节点"
          className="xflow-editor-rerun__select"
          getPopupContainer={getContainer}
          options={nodes.map((node) => ({ label: node.name, value: node.name }))}
          placeholder={nodes.length === 0 ? "该执行没有节点快照" : undefined}
          value={fromNode}
          onChange={(value) => setFromNode(value)}
        />
      </label>
      <p className="xflow-editor-rerun__hint">
        起点及其全部下游重新执行；上游节点的结果沿用原执行结果。
      </p>
      {successSource ? (
        <Checkbox
          checked={includeSuccess}
          onChange={(event) => setIncludeSuccess(event.target.checked)}
        >
          确认重跑已成功的执行
        </Checkbox>
      ) : null}
      {error ? (
        <p className="xflow-editor-rerun__error" role="alert">
          {error}
        </p>
      ) : null}
    </Modal>
  );
}
