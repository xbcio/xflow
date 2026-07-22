import type { CatalogItem } from "../types/editor";

export const CATALOG_CATEGORIES = {
  FLOW: "Flow",
  LOGIC: "Logic",
  INTEGRATION: "Integration",
  CONTROL: "Control",
} as const;

export const DEFAULT_CATALOG: CatalogItem[] = [
  { type: "start", label: "Start", category: CATALOG_CATEGORIES.FLOW },
  { type: "end", label: "End", category: CATALOG_CATEGORIES.FLOW },
  { type: "wait", label: "Wait", category: CATALOG_CATEGORIES.FLOW },
  { type: "merge", label: "Merge", category: CATALOG_CATEGORIES.FLOW },
  { type: "if", label: "If", category: CATALOG_CATEGORIES.LOGIC },
  { type: "switch", label: "Switch", category: CATALOG_CATEGORIES.LOGIC },
  { type: "http", label: "HTTP", category: CATALOG_CATEGORIES.INTEGRATION },
  { type: "grpc", label: "gRPC", category: CATALOG_CATEGORIES.INTEGRATION },
  { type: "function", label: "Function", category: CATALOG_CATEGORIES.INTEGRATION },
  { type: "database", label: "Database", category: CATALOG_CATEGORIES.INTEGRATION },
  { type: "approval", label: "Approval", category: CATALOG_CATEGORIES.CONTROL },
];

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
