// KeyValue: a free map `{ [key]: value }` edited as rows.
//
// Values are strings unless `valueType` names another registered component
// type (e.g. "ExpressionInput"), which is then used as the value editor. The
// type is looked up in the registry of the enclosing <Composer> — the Spec
// only carries the name (Doc B §5 "组件组合"); validateSpec checks it via
// `typeRefProps`.
//
// Rows live in local state so a row being typed (empty key, duplicate key,
// no value yet) is a draft: the map written through the value binding only
// contains complete rows, and nothing is written while a key is duplicated.
// Removing the last row writes {} which core turns into unset.

import { DeleteOutlined, PlusOutlined, WarningOutlined } from "@ant-design/icons";
import { Button, Input as AntInput } from "antd";
import { useId, useRef, useState, type ReactNode } from "react";
import { z } from "zod";
import { deepEqual, zodProps } from "../core";
import { useComposerRegistry, type ComposerComponent, type ComposerComponentProps } from "../react";
import { cx, FieldFrame, FieldLayoutContext, fieldShape, hintText, isPlainObject, isWrongShape, RawValue, safeJson, type FieldProps } from "./shared";
import { renderableOf } from "./renderable";

export interface KeyValueProps extends FieldProps {
  /** Registered component type used to edit each value; default: a text input. */
  valueType?: string;
  /** Props for the value editor (validated by that component's schema). */
  valueProps?: Record<string, unknown>;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  addText?: string;
}

interface Entry {
  id: number;
  key: string;
  value: unknown;
}

interface Rows {
  /** Value the rows were last reconciled with. */
  source: unknown;
  entries: Entry[];
}

function toEntries(value: unknown, nextId: () => number): Entry[] {
  return isPlainObject(value) ? Object.entries(value).map(([key, item]) => ({ id: nextId(), key, value: item })) : [];
}

/**
 * Keys core refuses to address (write rejects __proto__ / constructor /
 * prototype pointer tokens), and `map["__proto__"] = v` would set the
 * prototype instead of a key. Such rows stay drafts with a row error.
 */
const RESERVED_KEYS: ReadonlySet<string> = new Set(["__proto__", "constructor", "prototype"]);

/** The map the rows stand for; nothing is written while a key is duplicated or reserved. */
function toMap(entries: readonly Entry[]): { map: Record<string, unknown>; duplicates: Set<string>; reserved: boolean } {
  const map: Record<string, unknown> = {};
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const entry of entries) {
    if (entry.key === "" || RESERVED_KEYS.has(entry.key)) continue;
    if (seen.has(entry.key)) duplicates.add(entry.key);
    seen.add(entry.key);
    if (entry.value !== undefined && !duplicates.has(entry.key)) map[entry.key] = entry.value;
  }
  return { map, duplicates, reserved: entries.some((entry) => RESERVED_KEYS.has(entry.key)) };
}

/** Whether the value is what the rows say ({} and unset are the same map). */
const sameMap = (map: Record<string, unknown>, value: unknown) => deepEqual(map, isPlainObject(value) ? value : {});

/** Why a row is a draft (nothing of it is written), or undefined when it is complete. */
function rowErrorOf(entry: Entry, duplicates: ReadonlySet<string>): string | undefined {
  if (RESERVED_KEYS.has(entry.key)) return "保留键名，未保存";
  if (entry.key !== "" && duplicates.has(entry.key)) return "键重复，未保存";
  if (entry.key === "" && entry.value !== undefined) return "键为空，未保存";
  return undefined;
}

function KeyValueField({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<KeyValueProps>) {
  const registry = useComposerRegistry();
  const baseId = useId();
  const idRef = useRef(0);
  const nextId = () => ++idRef.current;
  const [rows, setRows] = useState<Rows>(() => ({ source: value, entries: toEntries(value, nextId) }));

  const editor = props.valueType ? registry?.get(props.valueType) : undefined;
  const acceptsValue = (item: unknown) => props.valueType !== undefined || typeof item === "string";
  const wrong = isWrongShape(value, (v) => isPlainObject(v) && Object.values(v).every(acceptsValue));

  // Reconcile with a changed value during render: keep drafts when the value
  // is what the rows already say (our own write echoed back).
  let current = rows;
  if (!Object.is(rows.source, value)) {
    current = sameMap(toMap(rows.entries).map, value)
      ? { source: value, entries: rows.entries }
      : { source: value, entries: toEntries(value, nextId) };
    setRows(current);
  }

  const commit = (entries: Entry[]) => {
    setRows({ source: value, entries });
    const { map, duplicates, reserved } = toMap(entries);
    if (duplicates.size > 0 || reserved) return;
    if (sameMap(map, value)) return;
    onChange?.(map);
  };
  const update = (id: number, patch: Partial<Entry>) =>
    commit(current.entries.map((entry) => (entry.id === id ? { ...entry, ...patch } : entry)));

  const { duplicates } = toMap(current.entries);
  // One error line per failing row, listed under the whole row list (baseline
  // §6.4); each keeps its own id for the row input's aria-describedby.
  const rowErrors = current.entries.flatMap((entry, index) => {
    const message = rowErrorOf(entry, duplicates);
    return message ? [{ id: `${baseId}row${entry.id}error`, index, message }] : [];
  });
  const unset = value === undefined;
  const editable = !readOnly && !props.disabled;
  const valueEditor = (entry: Entry, index: number): ReactNode => {
    const valueLabel = entry.key ? `${entry.key} 的值` : `第 ${index + 1} 行的值`;
    // Scalar-empty values ("" / null) leave the entry without a value (a draft).
    const onValue = (next: unknown) => update(entry.id, { value: next === "" || next === null ? undefined : next });
    if (!props.valueType) {
      return (
        <AntInput
          rootClassName="xflow-composer-control"
          aria-label={valueLabel}
          value={typeof entry.value === "string" ? entry.value : ""}
          placeholder={props.valuePlaceholder ?? "值"}
          readOnly={readOnly}
          disabled={props.disabled}
          onChange={(event) => onValue(event.target.value)}
        />
      );
    }
    if (!editor) {
      return (
        <span className="xflow-composer-inline-error" role="alert">
          未注册的组件类型 {props.valueType}
        </span>
      );
    }
    const parsed = editor.props.parse(props.valueProps ?? {});
    if (!parsed.ok) {
      return (
        <span className="xflow-composer-inline-error" role="alert">
          {props.valueType} 的 valueProps 无效：{parsed.issues.map((issue) => `${issue.path || "/"} ${issue.message}`).join("; ")}
        </span>
      );
    }
    const Render = renderableOf(editor);
    return (
      <div role="group" aria-label={valueLabel} className="xflow-composer-key-value-editor">
        <FieldLayoutContext.Provider value="inline">
          <Render
            node={{ id: `${node.id}/${entry.id}`, type: editor.type }}
            props={parsed.value}
            bindings={{ value: { value: entry.value, onChange: onValue } }}
            value={entry.value}
            onChange={onValue}
            issues={[]}
            slots={{}}
            readOnly={readOnly}
            emit={() => {}}
          />
        </FieldLayoutContext.Provider>
      </div>
    );
  };

  return (
    <FieldFrame
      className="xflow-composer-key-value"
      nodeId={node.id}
      {...props}
      issues={issues}
      hint={unset && defaultHint !== undefined ? hintText(defaultHint, (v) => safeJson(v)) : undefined}
      labelMode="group"
      stateClassName={unset ? "is-unset" : undefined}
    >
      {() =>
        wrong ? (
          <RawValue
            value={value}
            expected={props.valueType ? "对象" : "字符串映射"}
            readOnly={readOnly}
            onClear={() => onChange?.(undefined)}
          />
        ) : (
          <div className="xflow-composer-key-value-list">
            {current.entries.map((entry, index) => {
              const message = rowErrorOf(entry, duplicates);
              const errorId = `${baseId}row${entry.id}error`;
              return (
                <div key={entry.id} className={cx("xflow-composer-key-value-row", message && "is-error")}>
                  <AntInput
                    rootClassName="xflow-composer-control xflow-composer-key-value-key"
                    aria-label={`第 ${index + 1} 行的键`}
                    aria-describedby={message ? errorId : undefined}
                    value={entry.key}
                    placeholder={props.keyPlaceholder ?? "键"}
                    readOnly={readOnly}
                    disabled={props.disabled}
                    status={message ? "error" : undefined}
                    aria-invalid={message ? true : undefined}
                    onChange={(event) => update(entry.id, { key: event.target.value })}
                  />
                  <div className="xflow-composer-key-value-value">{valueEditor(entry, index)}</div>
                  {editable ? (
                    <Button
                      size="small"
                      type="text"
                      className="xflow-composer-icon-button"
                      icon={<DeleteOutlined />}
                      aria-label={entry.key ? `删除 ${entry.key}` : `删除第 ${index + 1} 行`}
                      onClick={() => commit(current.entries.filter((item) => item.id !== entry.id))}
                    />
                  ) : null}
                </div>
              );
            })}
            {rowErrors.map((error) => (
              <span key={error.id} id={error.id} className="xflow-composer-row-error" role="alert">
                <WarningOutlined aria-hidden className="xflow-composer-row-error-icon" />
                第 {error.index + 1} 行：{error.message}
              </span>
            ))}
            {current.entries.length === 0 ? <div className="xflow-composer-empty">暂无条目</div> : null}
            {editable ? (
              <Button
                size="small"
                className="xflow-composer-add xflow-composer-key-value-add"
                icon={<PlusOutlined />}
                onClick={() => setRows({ source: current.source, entries: [...current.entries, { id: nextId(), key: "", value: undefined }] })}
              >
                {props.addText ?? "添加"}
              </Button>
            ) : null}
          </div>
        )
      }
    </FieldFrame>
  );
}

export const KeyValue: ComposerComponent<KeyValueProps> = {
  type: "KeyValue",
  props: zodProps(
    z.strictObject({
      ...fieldShape,
      valueType: z.string().min(1).optional(),
      valueProps: z.record(z.string(), z.unknown()).optional(),
      keyPlaceholder: z.string().optional(),
      valuePlaceholder: z.string().optional(),
      addText: z.string().optional()
    }),
    { typeRefProps: ["valueType"] }
  ),
  bindings: { value: "object" },
  render: (p) => <KeyValueField {...p} />
};
