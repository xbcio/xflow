import { Button, Input, Modal, Select } from "antd";
import type { WorkflowParam } from "@xflow/core";
import * as React from "react";

import {
  declaredRunParams,
  initialRunParamValues,
  parseRunParams,
  type RunParamField,
  type RunParamValues
} from "./runParams";

/**
 * What the editor hands the host's `onRun` beside the definition. `params` is
 * present only when the workflow declares run parameters and the user filled
 * the run dialog; the host forwards it as the `params` of the execute request.
 */
export interface XFlowRunOptions {
  params?: Record<string, unknown>;
}

/**
 * One opening of the run dialog. `error` is the refusal of the last attempt:
 * the dialog stays open on it, so a server-side rejection never costs the user
 * what they typed.
 */
export interface RunParamsRequest {
  /** The workflow's declared `params` schema as it was when the dialog opened. */
  params: Record<string, WorkflowParam>;
  /** The refusal of the last attempt; the dialog stays open and shows it. */
  error?: string;
}

export interface RunParamsDialogProps {
  /** The dialog is open while a request is set. */
  request?: RunParamsRequest;
  workflowName?: string;
  /** A run is in flight: the form is inert and the dialog cannot be dismissed. */
  submitting?: boolean;
  getContainer?: () => HTMLElement;
  onCancel: () => void;
  /** Any field edit: the editor drops the previous refusal, which no longer describes the input. */
  onEdit: () => void;
  /** Called with the validated params. The editor starts the run and closes the dialog on success. */
  onSubmit: (options: XFlowRunOptions) => void;
}

const BOOLEAN_OPTIONS = [
  { label: "是", value: "true" },
  { label: "否", value: "false" }
];

const KIND_HINT: Record<RunParamField["kind"], string | undefined> = {
  string: undefined,
  number: "数字",
  boolean: undefined,
  object: "JSON 对象",
  array: "JSON 数组",
  json: "JSON"
};

/** onMouseDown handler that leaves focus where it is; the click still fires. */
function keepFieldFocus(event: React.MouseEvent): void {
  event.preventDefault();
}

function RunParamsForm({
  fields,
  error: submitError,
  submitting,
  getContainer,
  onCancel,
  onEdit,
  onSubmit
}: {
  fields: RunParamField[];
  error?: string;
  submitting: boolean;
  getContainer?: () => HTMLElement;
  onCancel: () => void;
  onEdit: () => void;
  onSubmit: (options: XFlowRunOptions) => void;
}): React.ReactElement {
  const baseId = React.useId();
  const formRef = React.useRef<HTMLFormElement>(null);
  // The form holds raw text. Nothing is parsed into a request until every
  // field passes, so a half-typed number or JSON never leaves the dialog.
  const [values, setValues] = React.useState<RunParamValues>(() => initialRunParamValues(fields));
  const [touched, setTouched] = React.useState<Record<string, boolean>>({});
  const [attempted, setAttempted] = React.useState(false);

  const parsed = React.useMemo(() => parseRunParams(fields, values), [fields, values]);
  const idOf = (name: string): string => `${baseId}-${name}`;
  const errorOf = (name: string): string | undefined =>
    attempted || touched[name] ? parsed.errors[name] : undefined;
  const setValue = (name: string, value: string) => {
    setValues((current) => ({ ...current, [name]: value }));
    onEdit();
  };
  const touch = (name: string) => setTouched((current) => (current[name] ? current : { ...current, [name]: true }));

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    if (submitting) return;
    setAttempted(true);
    if (Object.keys(parsed.errors).length > 0) {
      // Errors render on the next paint; focus the first offending control then.
      window.requestAnimationFrame(() => {
        formRef.current?.querySelector<HTMLElement>('[aria-invalid="true"]')?.focus();
      });
      return;
    }
    onSubmit({ params: parsed.params });
  };

  const renderControl = (field: RunParamField, index: number) => {
    const id = idOf(field.name);
    const error = errorOf(field.name);
    const common = {
      "aria-describedby": error ? `${id}-error` : undefined,
      "aria-invalid": error ? true : undefined,
      "aria-required": field.required || undefined,
      disabled: submitting,
      id,
      status: error ? ("error" as const) : undefined
    };
    if (field.kind === "boolean") {
      return (
        <Select
          {...common}
          allowClear={!field.required}
          autoFocus={index === 0}
          className="xflow-editor-run-params__select"
          getPopupContainer={getContainer}
          options={BOOLEAN_OPTIONS}
          placeholder="请选择"
          value={values[field.name] || undefined}
          onBlur={() => touch(field.name)}
          onChange={(next: string | undefined) => setValue(field.name, next ?? "")}
        />
      );
    }
    if (field.kind === "string" || field.kind === "number") {
      return (
        <Input
          {...common}
          autoComplete="off"
          autoFocus={index === 0}
          inputMode={field.kind === "number" ? "decimal" : undefined}
          value={values[field.name] ?? ""}
          onBlur={() => touch(field.name)}
          onChange={(event) => setValue(field.name, event.target.value)}
        />
      );
    }
    return (
      <Input.TextArea
        {...common}
        autoFocus={index === 0}
        autoSize={{ minRows: 3, maxRows: 8 }}
        className="xflow-editor-run-params__json"
        spellCheck={false}
        value={values[field.name] ?? ""}
        onBlur={() => touch(field.name)}
        onChange={(event) => setValue(field.name, event.target.value)}
      />
    );
  };

  const keyDown = (event: React.KeyboardEvent<HTMLFormElement>) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      // ⌘/Ctrl+Enter submits from any field, a multi-line JSON box included.
      event.preventDefault();
      event.stopPropagation();
      formRef.current?.requestSubmit();
      return;
    }
    // A printable key typed into a field belongs to the field. The editor's
    // window-level bindings (`?` opens the shortcut sheet) must not swallow it,
    // while Esc and Tab still reach the dialog's own focus trap.
    if (event.key.length === 1 && !event.metaKey && !event.ctrlKey && !event.altKey) {
      event.stopPropagation();
    }
  };

  return (
    <form
      className="xflow-editor-run-params__form"
      id={`${baseId}-form`}
      noValidate
      ref={formRef}
      onKeyDown={keyDown}
      onSubmit={submit}
    >
      {fields.map((field, index) => {
        const id = idOf(field.name);
        const error = errorOf(field.name);
        const hint = KIND_HINT[field.kind];
        return (
          <div className="xflow-editor-run-params__field" data-param={field.name} key={field.name}>
            <label className="xflow-editor-run-params__label" htmlFor={id}>
              <span>{field.label}</span>
              {field.label !== field.name ? (
                <code className="xflow-editor-run-params__name">{field.name}</code>
              ) : null}
              {field.required ? (
                <span aria-hidden="true" className="xflow-editor-run-params__required">
                  *
                </span>
              ) : null}
              {hint ? <span className="xflow-editor-run-params__kind">{hint}</span> : null}
            </label>
            {renderControl(field, index)}
            {error ? (
              <p className="xflow-editor-run-params__error" id={`${id}-error`}>
                {error}
              </p>
            ) : null}
          </div>
        );
      })}
      {submitError ? (
        <p className="xflow-editor-run-params__error xflow-editor-run-params__error--submit" role="alert">
          {submitError}
        </p>
      ) : null}
      <div className="xflow-editor-run-params__actions">
        {/* Pressing a button must not blur the focused field: the blur reveals
            that field's error, the layout shifts under the pointer and the
            click is lost (only a real mouse shows this). Keyboard focus moves
            as usual. */}
        <Button disabled={submitting} onClick={onCancel} onMouseDown={keepFieldFocus}>
          取消
        </Button>
        <Button htmlType="submit" loading={submitting} type="primary" onMouseDown={keepFieldFocus}>
          运行
        </Button>
      </div>
    </form>
  );
}

/**
 * Run-parameters dialog. A workflow that declares top-level `params` cannot be
 * started without them (the server answers 400 `workflow_params_invalid`), so
 * the editor opens this instead of running at once. Undeclared workflows keep
 * the one-click run.
 */
export function RunParamsDialog({
  request,
  workflowName,
  submitting = false,
  getContainer,
  onCancel,
  onEdit,
  onSubmit
}: RunParamsDialogProps): React.ReactElement {
  const open = request !== undefined;
  const fields = React.useMemo(() => declaredRunParams(request?.params), [request?.params]);
  return (
    <Modal
      centered={false}
      className="xflow-editor-run-params"
      classNames={{
        body: "xflow-editor-run-params__body",
        container: "xflow-editor-run-params__content",
        header: "xflow-editor-run-params__header"
      }}
      closable={!submitting}
      destroyOnHidden
      footer={null}
      getContainer={getContainer}
      keyboard={!submitting}
      mask={{ closable: false }}
      open={open}
      style={{ top: 64 }}
      title={
        <span className="xflow-editor-run-params__title">
          <span>运行参数</span>
          {workflowName ? <span className="xflow-editor-run-params__sub">{workflowName}</span> : null}
        </span>
      }
      width={520}
      onCancel={submitting ? undefined : onCancel}
    >
      {request ? (
        <RunParamsForm
          error={request.error}
          fields={fields}
          getContainer={getContainer}
          submitting={submitting}
          onCancel={onCancel}
          onEdit={onEdit}
          onSubmit={onSubmit}
        />
      ) : null}
    </Modal>
  );
}
