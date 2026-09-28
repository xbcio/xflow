// Base64Input (Doc C §3 `base64`): a read-only summary of a base64 payload
// (e.g. a wasm module in script.code) plus "replace from file" and "clear".
// The text itself is never put into a form control: a module is megabytes of
// base64 that no one edits by hand, and rendering it in a textarea stalls the
// Inspector. Hand edits go through the JSON tab.

import { Button } from "antd";
import { useRef, useState } from "react";
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import { FieldFrame, RawValue } from "@xflow/composer/form";
import type { ComposerComponent, ComposerComponentProps } from "@xflow/composer/react";
import { chromeShape, cx, JSON_TAB_HINT, NODEFORM, type ChromeProps } from "./shared";

const BASE64 = /^[A-Za-z0-9+/]*={0,2}$/;
/** base64 of the wasm magic + version 1: "\0asm\x01\0\0\0". */
const WASM_V1_PREFIX = "AGFzbQEAAAA";
const WASM_PREFIX = "AGFzbQ";
const PREVIEW_CHARS = 24;

export interface Base64Summary {
  /** Characters, whitespace excluded. */
  length: number;
  valid: boolean;
  /** Decoded size in bytes; undefined when not valid base64. */
  bytes?: number;
  wasm: boolean;
  preview: string;
}

/** Reads a base64 string without decoding it. Whitespace is ignored. */
export function summarizeBase64(text: string): Base64Summary {
  const compact = text.replace(/\s+/g, "");
  const valid = compact.length > 0 && compact.length % 4 === 0 && BASE64.test(compact);
  const padding = compact.endsWith("==") ? 2 : compact.endsWith("=") ? 1 : 0;
  return {
    length: compact.length,
    valid,
    ...(valid && { bytes: (compact.length / 4) * 3 - padding }),
    wasm: compact.startsWith(WASM_PREFIX),
    preview: compact.length > PREVIEW_CHARS ? `${compact.slice(0, PREVIEW_CHARS)}…` : compact
  };
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

/** base64 of raw bytes, chunked so large modules do not overflow the call stack. */
export function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

/** Reads a file's bytes (FileReader, which every host — jsdom included — has). */
function readBytes(file: Blob): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
    reader.onerror = () => reject(reader.error ?? new Error("read failed"));
    reader.readAsArrayBuffer(file);
  });
}

function Base64Field({ node, props, value, onChange, issues, readOnly }: ComposerComponentProps<ChromeProps>) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [readError, setReadError] = useState<string>();
  const wrong = value !== undefined && typeof value !== "string";
  const text = typeof value === "string" ? value : "";
  const summary = text === "" ? undefined : summarizeBase64(text);
  const locked = readOnly || props.disabled;

  const pick = () => fileRef.current?.click();
  const onFile = async (file: File | undefined) => {
    if (!file) return;
    try {
      setReadError(undefined);
      onChange?.(bytesToBase64(await readBytes(file)));
    } catch {
      setReadError(`无法读取文件 ${file.name}`);
    }
  };

  const note = readError
    ? { tone: "warning", text: readError }
    : summary && !summary.valid
      ? { tone: "warning", text: "当前值不是合法的 base64" }
      : undefined;

  return (
    <FieldFrame
      className={`${NODEFORM}-base64`}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={props.disabled}
      issues={issues}
      hint={note ? <span className={cx(`${NODEFORM}-note`, `is-${note.tone}`)}>{note.text}</span> : undefined}
      labelMode="group"
      groupRole="group"
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected="base64 字符串" readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <div className={`${NODEFORM}-base64-body`}>
            <div className={`${NODEFORM}-base64-summary`} id={ids.controlId} role="status" aria-describedby={ids.describedBy}>
              {summary ? (
                <>
                  <span className={`${NODEFORM}-base64-kind`}>
                    {summary.wasm ? (text.replace(/\s+/g, "").startsWith(WASM_V1_PREFIX) ? "WebAssembly 模块 v1" : "WebAssembly 模块") : "二进制数据"}
                  </span>
                  <span className={`${NODEFORM}-base64-size`}>
                    {summary.bytes !== undefined ? formatBytes(summary.bytes) : `${summary.length} 个字符`}
                  </span>
                  <code className={`${NODEFORM}-base64-preview`}>{summary.preview}</code>
                </>
              ) : (
                <span className={`${NODEFORM}-base64-empty`}>未设置</span>
              )}
            </div>
            {locked ? null : (
              <div className={`${NODEFORM}-base64-actions`}>
                <Button size="small" onClick={pick}>
                  {summary ? "替换文件…" : "选择文件…"}
                </Button>
                {value !== undefined ? (
                  <Button size="small" autoInsertSpace={false} onClick={() => onChange?.(undefined)}>
                    清除
                  </Button>
                ) : null}
                <input
                  ref={fileRef}
                  type="file"
                  hidden
                  aria-label={`为 ${props.label ?? "该字段"} 选择文件`}
                  onChange={(event) => {
                    const input = event.currentTarget;
                    void onFile(input.files?.[0]).finally(() => {
                      input.value = "";
                    });
                  }}
                />
              </div>
            )}
            <p className={`${NODEFORM}-base64-help`}>内容以 base64 保存，不在表单中展开；如需编辑原文，{JSON_TAB_HINT}。</p>
          </div>
        )
      }
    </FieldFrame>
  );
}

export const Base64Input: ComposerComponent<ChromeProps> = {
  type: "Base64Input",
  props: zodProps(z.strictObject(chromeShape)),
  bindings: { value: "scalar" },
  render: (p) => <Base64Field {...p} />
};
