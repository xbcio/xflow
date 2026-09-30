// Node-form catalogs by locale (see ../i18n.ts).

import type { NodeFormCatalog } from "../i18n";
import { zhCN } from "./zh-CN";

export const nodeFormCatalogs = { "zh-CN": zhCN } as const satisfies Record<string, NodeFormCatalog>;
