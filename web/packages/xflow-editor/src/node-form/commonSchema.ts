// Fields every WorkflowNode has, independent of its type (Doc C §4.2).
// compileNodeForm places them before the type schema.
//
// Wire shapes (types/workflow.go NodeDef / RetrySettings; xflow-core
// WorkflowNode / RetryPolicy):
// - on_error: types.OnError — stop | error_output | main_output | continue.
// - timeout: time.Duration with omitempty → integer NANOSECONDS; 0/unset
//   inherits engine.DefaultNodeTimeout, negative means no limit.
// - retry.initial_interval / max_interval: int MILLISECONDS — engine/retry.go
//   multiplies by time.Millisecond. (Doc C §4.2 says "ns 模式"; the code says
//   ms, so the ms unit is used here. See the C1 report.)
// - runner_selector: node level only accepts match_labels; a node-level
//   `mode` is a compile error (engine/graph validateNodeRunnerSelector), so
//   the form does not offer it.
//
// `/type`, `/activation_replicas`, `/output`, `/inputs` stay on the editor's
// existing controls (Doc C §4.2).
//
// The widgets "node-name", "duration-ns" and "duration-ms" are private to this
// schema (compile.ts maps them); backend schemas never use them.

import type { NodeFormSchema } from "./schema";

export const COMMON_WIDGET_NODE_NAME = "node-name";
export const COMMON_WIDGET_DURATION_NS = "duration-ns";
export const COMMON_WIDGET_DURATION_MS = "duration-ms";

export const commonSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "$common",
  node_version: 0,
  kind: "action",
  groups: [
    { key: "general", display_name: "通用" },
    { key: "execution", display_name: "执行策略", collapsed: true }
  ],
  fields: [
    {
      name: "name",
      path: "/name",
      label: "名称",
      type: "string",
      widget: COMMON_WIDGET_NODE_NAME,
      help: "失焦时改名，并同步更新连线、pin_data、分组与依赖边中的引用",
      group: "general",
      order: 10
    },
    {
      name: "disabled",
      path: "/disabled",
      label: "禁用",
      type: "boolean",
      widget: "switch",
      default: false,
      group: "general",
      order: 20
    },
    {
      name: "notes",
      path: "/notes",
      label: "备注",
      type: "string",
      widget: "textarea",
      group: "general",
      order: 30
    },
    {
      name: "on_error",
      path: "/on_error",
      label: "错误处理",
      type: "string",
      widget: "select",
      help: "未设置时沿用工作流 settings.on_error",
      options: [
        { value: "stop", label: "停止", description: "节点失败时终止执行" },
        { value: "error_output", label: "错误端口", description: "把错误路由到 error 输出端口" },
        { value: "main_output", label: "主端口", description: "把错误作为结果从 main 端口输出" },
        { value: "continue", label: "继续", description: "忽略错误继续执行" }
      ],
      group: "execution",
      order: 10
    },
    {
      name: "timeout",
      path: "/timeout",
      label: "超时",
      type: "number",
      widget: COMMON_WIDGET_DURATION_NS,
      help: "单次执行的超时（线上为纳秒整数）；未设置继承引擎默认值，负值表示不限",
      group: "execution",
      order: 20
    },
    {
      name: "retry",
      path: "/retry",
      label: "重试",
      type: "object",
      widget: "object-group",
      help: "覆盖工作流 settings.retry；未设置时继承",
      group: "execution",
      order: 30,
      fields: [
        {
          name: "enabled",
          path: "/retry/enabled",
          label: "启用",
          type: "boolean",
          widget: "switch",
          // engine/graph resolveRetry keys on max_attempts > 0; enabled is not read.
          help: "引擎目前不读取此字段，是否重试由最大尝试次数决定",
          order: 10
        },
        {
          name: "max_attempts",
          path: "/retry/max_attempts",
          label: "最大尝试次数",
          type: "number",
          help: "大于 0 时启用节点级重试；否则沿用工作流级重试",
          order: 20
        },
        {
          name: "strategy",
          path: "/retry/strategy",
          label: "策略",
          type: "string",
          options: [
            { value: "fixed", label: "固定间隔" },
            { value: "exponential", label: "指数退避" }
          ],
          order: 30
        },
        {
          name: "initial_interval",
          path: "/retry/initial_interval",
          label: "初始间隔",
          type: "number",
          widget: COMMON_WIDGET_DURATION_MS,
          help: "毫秒整数",
          // engine/retry.go: base defaults to time.Second.
          fallback: 1000,
          order: 40
        },
        {
          name: "max_interval",
          path: "/retry/max_interval",
          label: "最大间隔",
          type: "number",
          widget: COMMON_WIDGET_DURATION_MS,
          help: "毫秒整数；单次等待另有 5 分钟上限",
          // engine/retry.go: retryBackoffCap = 5 * time.Minute.
          fallback: 300000,
          order: 50
        },
        {
          name: "multiplier",
          path: "/retry/multiplier",
          label: "倍率",
          type: "number",
          // engine/retry.go: multiplier defaults to 2.0.
          fallback: 2,
          order: 60
        }
      ]
    },
    {
      name: "runner_selector",
      path: "/runner_selector",
      label: "Runner 选择",
      type: "object",
      widget: "object-group",
      help: "节点级只支持 match_labels；mode 只能写在工作流级",
      group: "execution",
      order: 40,
      fields: [
        {
          name: "match_labels",
          path: "/runner_selector/match_labels",
          label: "匹配标签",
          type: "object",
          widget: "key-value",
          order: 10
        }
      ]
    }
  ]
};
