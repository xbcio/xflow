import type { ReactNode } from "react";
import type { CatalogItem } from "../types/editor";

export const CATALOG_CATEGORIES = {
  CONTROL: "控制",
  FLOW: "流程",
  ACTION: "动作",
  WAIT: "等待/延时",
} as const;

const icons: Record<string, ReactNode> = {
  start: (
    <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
      <path d="M8 5v14l11-7z" />
    </svg>
  ),
  end: (
    <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
      <path d="M6 6h12v12H6z" />
    </svg>
  ),
  wait: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  ),
  merge: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4" />
    </svg>
  ),
  if: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4" />
    </svg>
  ),
  switch: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
    </svg>
  ),
  approval: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
    </svg>
  ),
  http: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9" />
    </svg>
  ),
  grpc: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
    </svg>
  ),
  function: (
    <span className="text-[10px] font-semibold uppercase tracking-wide">Fn</span>
  ),
  database: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
    </svg>
  ),
};

export const DEFAULT_CATALOG: CatalogItem[] = [
  { type: "start", label: "开始", category: CATALOG_CATEGORIES.FLOW },
  { type: "end", label: "结束", category: CATALOG_CATEGORIES.FLOW },
  { type: "merge", label: "合并", category: CATALOG_CATEGORIES.FLOW },
  { type: "if", label: "条件", category: CATALOG_CATEGORIES.CONTROL },
  { type: "switch", label: "分支", category: CATALOG_CATEGORIES.CONTROL },
  { type: "approval", label: "审批", category: CATALOG_CATEGORIES.CONTROL },
  { type: "http", label: "HTTP 请求", category: CATALOG_CATEGORIES.ACTION },
  { type: "grpc", label: "gRPC 请求", category: CATALOG_CATEGORIES.ACTION },
  { type: "function", label: "函数", category: CATALOG_CATEGORIES.ACTION },
  { type: "database", label: "查询数据库", category: CATALOG_CATEGORIES.ACTION },
  { type: "wait", label: "延时", category: CATALOG_CATEGORIES.WAIT },
];

export function getCatalogIcon(type: string): ReactNode {
  return icons[type] ?? null;
}

export function filterCatalog(
  catalog: CatalogItem[],
  keyword: string
): CatalogItem[] {
  const normalized = keyword.trim().toLowerCase();
  if (!normalized) {
    return catalog;
  }

  return catalog.filter(
    (item) =>
      item.label.toLowerCase().includes(normalized) ||
      item.type.toLowerCase().includes(normalized) ||
      item.category.toLowerCase().includes(normalized)
  );
}
