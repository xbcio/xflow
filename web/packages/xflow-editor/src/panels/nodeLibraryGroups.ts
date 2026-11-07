/**
 * The node library's group vocabulary, in display order (§6.0.1). "其他"
 * collects types with no presentation entry. Kept beside the library so the
 * panel does not import the editor shell back (that would be a cycle).
 */
export const nodeLibraryGroups = ["触发器", "流程控制", "动作", "代码与转换", "人工", "其他"] as const;

export type NodeLibraryGroup = (typeof nodeLibraryGroups)[number];
