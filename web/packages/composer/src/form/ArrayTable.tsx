// ArrayTable: the repeat container (Doc B §3.4). It renders one table row
// per `$row` (keyed by the row id, so the other rows keep their DOM and
// local state across add / remove / reorder) and changes the list only by
// writing the whole array through its `value` binding. Removing the last
// row writes [] which core turns into unset.

import { ArrowDownOutlined, ArrowUpOutlined, DeleteOutlined, PlusOutlined } from "@ant-design/icons";
import { Button } from "antd";
import { z } from "zod";
import { zodProps } from "../core";
import type { ComposerComponent, ComposerComponentProps } from "../react";
import { FieldFrame, FieldLayoutContext, fieldShape, hintText, isWrongShape, RawValue, safeJson, type FieldProps } from "./shared";

export interface ArrayTableProps extends FieldProps {
  /** Column headers, in the order of the row elements. */
  columns?: string[];
  addText?: string;
  emptyText?: string;
  /** JSON template of a new row; default {}. Deep-copied on every add. */
  newRow?: unknown;
  /** Show move up / down actions; default true. */
  sortable?: boolean;
  minItems?: number;
  maxItems?: number;
}

function cloneJson<T>(value: T): T {
  return value === undefined ? value : (JSON.parse(JSON.stringify(value)) as T);
}

function ArrayTableField({ node, props, value, onChange, defaultHint, issues, readOnly, rows }: ComposerComponentProps<ArrayTableProps>) {
  const wrong = isWrongShape(value, (v) => Array.isArray(v) || v === null);
  const list: unknown[] = Array.isArray(value) ? value : [];
  const editable = !readOnly && !props.disabled && onChange !== undefined;
  const sortable = props.sortable !== false;
  const canAdd = editable && (props.maxItems === undefined || list.length < props.maxItems);
  const canRemove = editable && (props.minItems === undefined || list.length > props.minItems);
  const unset = value === undefined || value === null;

  const move = (index: number, delta: number) => {
    const target = index + delta;
    if (target < 0 || target >= list.length) return;
    const next = [...list];
    [next[index], next[target]] = [next[target], next[index]];
    onChange?.(next);
  };

  return (
    <FieldFrame
      className="xflow-composer-array-table"
      nodeId={node.id}
      {...props}
      issues={issues}
      hint={unset && defaultHint !== undefined ? hintText(defaultHint, (v) => safeJson(v)) : undefined}
      labelMode="group"
      stateClassName={unset ? "is-unset" : undefined}
    >
      {() => {
        if (wrong) return <RawValue value={value} expected="数组" readOnly={readOnly} onClear={() => onChange?.(undefined)} />;
        if (rows === undefined) {
          // Not a repeat container: nothing to render rows with.
          return (
            <div className="xflow-composer-raw">
              <p className="xflow-composer-raw-notice" role="note">
                ArrayTable 需要配置 repeat 才能编辑行。
              </p>
              {value !== undefined ? <pre className="xflow-composer-raw-value">{safeJson(value, 2)}</pre> : null}
            </div>
          );
        }
        const columns = props.columns ?? [];
        return (
          <div className="xflow-composer-array">
            <div className="xflow-composer-array-grid" role="table" aria-rowcount={rows.length + (columns.length > 0 ? 1 : 0)}>
              {columns.length > 0 ? (
                <div className="xflow-composer-array-row xflow-composer-array-head" role="row">
                  {columns.map((column, index) => (
                    <div key={index} className="xflow-composer-array-heading" role="columnheader">
                      {column}
                    </div>
                  ))}
                  {editable ? (
                    <div className="xflow-composer-array-heading xflow-composer-array-actions" role="columnheader">
                      <span className="xflow-composer-visually-hidden">操作</span>
                    </div>
                  ) : null}
                </div>
              ) : null}
              {rows.map((row) => (
                <div key={row.id} className="xflow-composer-array-row" role="row" data-composer-row={row.id}>
                  <FieldLayoutContext.Provider value="cell">{row.children}</FieldLayoutContext.Provider>
                  {editable ? (
                    <div className="xflow-composer-array-actions" role="cell">
                      {sortable ? (
                        <>
                          <Button
                            size="small"
                            type="text"
                            className="xflow-composer-icon-button"
                            icon={<ArrowUpOutlined />}
                            aria-label={`上移第 ${row.index + 1} 行`}
                            disabled={row.index === 0}
                            onClick={() => move(row.index, -1)}
                          />
                          <Button
                            size="small"
                            type="text"
                            className="xflow-composer-icon-button"
                            icon={<ArrowDownOutlined />}
                            aria-label={`下移第 ${row.index + 1} 行`}
                            disabled={row.index === rows.length - 1}
                            onClick={() => move(row.index, 1)}
                          />
                        </>
                      ) : null}
                      <Button
                        size="small"
                        type="text"
                        danger
                        className="xflow-composer-icon-button"
                        icon={<DeleteOutlined />}
                        aria-label={`删除第 ${row.index + 1} 行`}
                        disabled={!canRemove}
                        onClick={() => onChange?.(list.filter((_, index) => index !== row.index))}
                      />
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
            {rows.length === 0 ? <div className="xflow-composer-empty">{props.emptyText ?? "暂无数据"}</div> : null}
            {canAdd ? (
              <Button
                size="small"
                type="dashed"
                className="xflow-composer-add"
                icon={<PlusOutlined />}
                onClick={() => onChange?.([...list, cloneJson(props.newRow ?? {})])}
              >
                {props.addText ?? "添加"}
              </Button>
            ) : null}
          </div>
        );
      }}
    </FieldFrame>
  );
}

export const ArrayTable: ComposerComponent<ArrayTableProps> = {
  type: "ArrayTable",
  props: zodProps(
    z.strictObject({
      ...fieldShape,
      columns: z.array(z.string()).optional(),
      addText: z.string().optional(),
      emptyText: z.string().optional(),
      newRow: z.unknown().optional(),
      sortable: z.boolean().optional(),
      minItems: z.number().int().nonnegative().optional(),
      maxItems: z.number().int().positive().optional()
    })
  ),
  bindings: { value: "array" },
  render: (p) => <ArrayTableField {...p} />
};
