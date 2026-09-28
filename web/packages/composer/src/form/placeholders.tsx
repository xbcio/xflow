// Unsupported / ElementError: replace composer/react's built-in
// placeholders (renderNode looks them up by type). They keep the same
// `data-composer-placeholder` hooks, show the props-schema issues of an
// ElementError and, when the element has a bound value, that value as
// read-only JSON so nothing is hidden from the user. They never write.

import { Fragment, type ReactNode } from "react";
import { z } from "zod";
import { zodProps } from "../core";
import type { ComposerComponent, ComposerComponentProps, PlaceholderProps } from "../react";
import { safeJson } from "./shared";

const placeholderSchema = zodProps<PlaceholderProps>(
  z.object({
    type: z.string(),
    message: z.string(),
    error: z
      .object({
        code: z.string(),
        message: z.string(),
        issues: z.array(z.object({ path: z.string(), message: z.string() })).optional()
      })
      .optional()
  })
);

type ErrorDetail = { code: string; message: string; issues?: { path: string; message: string }[] };

function PlaceholderBody({
  kind,
  title,
  p
}: {
  kind: "unsupported" | "error";
  title: string;
  p: ComposerComponentProps<PlaceholderProps>;
}): ReactNode {
  const error = p.props.error as ErrorDetail | undefined;
  const bound = Object.entries(p.bindings).filter(([, binding]) => binding.value !== undefined);
  return (
    <div
      className={kind === "unsupported" ? "xflow-composer-unsupported" : "xflow-composer-element-error"}
      data-composer-placeholder={kind}
      data-composer-node={p.node.id}
      role={kind === "unsupported" ? "note" : "alert"}
    >
      <div className="xflow-composer-placeholder-title">{title}</div>
      <div className="xflow-composer-placeholder-message">{p.props.message}</div>
      {error?.issues?.length ? (
        <ul className="xflow-composer-placeholder-issues">
          {error.issues.map((issue, index) => (
            <li key={index}>
              <code>{issue.path || "/"}</code> {issue.message}
            </li>
          ))}
        </ul>
      ) : null}
      {bound.map(([name, binding]) => (
        <pre key={name} className="xflow-composer-raw-value" aria-label={`${name} 的当前值`} tabIndex={0}>
          {safeJson(binding.value, 2)}
        </pre>
      ))}
      {p.children}
      {Object.entries(p.slots).map(([name, slot]) => (
        <Fragment key={name}>{slot}</Fragment>
      ))}
    </div>
  );
}

export const Unsupported: ComposerComponent<PlaceholderProps> = {
  type: "Unsupported",
  props: placeholderSchema,
  bindings: {},
  render: (p) => <PlaceholderBody kind="unsupported" title={`不支持的组件：${p.props.type || "(无类型)"}`} p={p} />
};

export const ElementError: ComposerComponent<PlaceholderProps> = {
  type: "ElementError",
  props: placeholderSchema,
  bindings: {},
  render: (p) => <PlaceholderBody kind="error" title={`组件 ${p.props.type || "(无类型)"} 配置有误`} p={p} />
};
