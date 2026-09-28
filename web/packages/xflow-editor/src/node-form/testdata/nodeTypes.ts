// Hand-derived NodeFormSchema fixtures (C1). /v1/node-types (A6) does not
// exist yet, so these are transcribed from the Go descriptors committed at
// 094d4a5 ("feat(node): declare editor metadata on builtin descriptors"):
//
//   xflow.wait                 node/internal/flow/wait.go            WaitNode.Descriptor
//   xflow.switch               node/internal/flow/switch.go          Descriptor
//   xflow.map                  node/internal/flow/map.go             Descriptor
//   xflow.script               node/internal/code/script/script.go   Descriptor
//   xflow.http                 node/internal/action/http.go          Descriptor
//   xflow.database             node/internal/action/database.go      Descriptor
//   xflow.trigger.kafka        node/trigger/kafka/kafka.go           Descriptor
//   xflow.trigger.redis        node/trigger/redis/redis.go           Descriptor
//   xflow.transform.aggregate  node/internal/transform/aggregate.go  Descriptor
//
// Projection rules applied by hand (what A6 is expected to do):
// - ParamSpec.DisplayName → label, Description → help, Default → default.
// - Constraints.Format → rule {type:"format"}; url/host-port are advisory.
//   Constraints.Pattern → rule {type:"pattern"}; Min → rule {type:"min"}.
// - Condition{Param, Eq, In, Truthy, AllOf, Not} → snake_case Condition;
//   nodeinternal.CondNever() is {not: {}}.
// - No `expression` key: A6 will serve ExpressionMode; until then the
//   compiler derives it (expressionMode.ts). That is the path under test.
// - switch `ports.dynamic_outputs` has no Go metadata yet; it is the
//   hand-written expectation of Doc C §2.1 ("switch 类节点写成 …").
//
// Generated coverage of every builtin type now lives in
// testdata/node-types.generated.json (A6), exercised by
// nodeForm.generated.test.ts; these hand fixtures stay as focused cases.

import type { Condition, NodeFormField, NodeFormSchema } from "../schema";

const eq = (param: string, value: unknown): Condition => ({ param, eq: value });
const inList = (param: string, ...values: unknown[]): Condition => ({ param, in: values });
const truthy = (param: string): Condition => ({ param, truthy: true });
const falsy = (param: string): Condition => ({ param, truthy: false });
const all = (...conds: Condition[]): Condition => ({ all_of: conds });
const never: Condition = { not: {} };

type FieldInit = Omit<NodeFormField, "name" | "path">;
const param = (name: string, init: FieldInit): NodeFormField => ({ name, path: `/parameters/${name}`, ...init });
const sub = (parent: string, name: string, init: FieldInit): NodeFormField => ({ name, path: `${parent}/${name}`, ...init });
const itemField = (name: string, init: FieldInit): NodeFormField => ({ name, path: name, ...init });
const stringItem = { type: "string" as const };
const duration = { type: "format" as const, format: "duration" };
const expression = { type: "format" as const, format: "expression" };

export const waitSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.wait",
  node_version: 1,
  kind: "action",
  display_name: "Wait",
  ports: {
    inputs: [{ name: "main", display_name: "Main" }],
    outputs: [
      { name: "main", display_name: "Main" },
      { name: "timeout", display_name: "Timeout" },
      { name: "error", display_name: "Error" }
    ]
  },
  fields: [
    param("mode", {
      label: "Mode",
      type: "string",
      default: "signal",
      help: 'Trigger mode: "signal" (default) or "timer"',
      options: [
        { value: "signal", label: "Signal", description: "Wait for one or more external signals" },
        { value: "timer", label: "Timer", description: "Wait for a fixed duration or until an absolute time" }
      ]
    }),
    param("signal_name", {
      label: "Signal Name",
      type: "string",
      help: "Name of the external signal to wait for (signal mode)",
      visible_when: inList("mode", "signal", null)
    }),
    param("signals", {
      label: "Signal Names",
      type: "array",
      help: "List of signal names to wait for (multi-signal mode)",
      item: stringItem,
      visible_when: inList("mode", "signal", null)
    }),
    param("quorum", {
      label: "Quorum",
      type: "number",
      help: "Number of signals required to proceed; 0 or unset means all (multi-signal mode)",
      visible_when: all(inList("mode", "signal", null), truthy("signals"))
    }),
    param("timeout", {
      label: "Timeout",
      type: "string",
      widget: "duration",
      help: 'Maximum wait duration before routing to timeout port (e.g. "48h")',
      rules: [duration]
    }),
    param("duration", {
      label: "Duration",
      type: "string",
      widget: "duration",
      help: 'Fixed wait duration (timer mode, e.g. "5m")',
      rules: [duration],
      visible_when: eq("mode", "timer"),
      required_when: all(eq("mode", "timer"), falsy("until"))
    }),
    param("until", {
      label: "Until",
      type: "string",
      widget: "datetime",
      help: "Absolute time expression to wait until (timer mode)",
      visible_when: eq("mode", "timer"),
      required_when: all(eq("mode", "timer"), falsy("duration"))
    })
  ]
};

export const switchSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.switch",
  node_version: 1,
  kind: "action",
  display_name: "Switch",
  ports: { inputs: [{ name: "main", display_name: "Main" }], outputs: [], dynamic_outputs: { from: "/parameters/outputs" } },
  fields: [
    param("mode", {
      label: "Mode",
      type: "string",
      required: true,
      help: 'Routing mode: "rules" or "expression"',
      options: [
        { value: "rules", label: "Rules", description: "First rule whose condition holds picks the output" },
        { value: "expression", label: "Expression", description: "Expression result is the output port name" }
      ]
    }),
    param("outputs", { label: "Outputs", type: "array", required: true, help: "List of output port names (dynamic)", item: stringItem }),
    param("rules", {
      label: "Rules",
      type: "array",
      help: "Rule list for rules mode",
      visible_when: inList("mode", "rules", null),
      item: {
        type: "object",
        fields: [
          itemField("condition", {
            label: "Condition",
            type: "string",
            widget: "expression",
            help: "Boolean expression; a rule with an empty condition is skipped",
            rules: [expression]
          }),
          itemField("output", {
            label: "Output",
            type: "string",
            widget: "port-select",
            help: "Output port taken when condition holds; a rule with an empty output is skipped"
          })
        ]
      }
    }),
    param("expression", {
      label: "Expression",
      type: "string",
      widget: "expression",
      help: "Expression for expression mode",
      rules: [expression],
      visible_when: eq("mode", "expression"),
      required_when: eq("mode", "expression")
    }),
    param("default_output", { label: "Default Output", type: "string", widget: "port-select", help: "Port name used when no rule matches" })
  ]
};

export const mapSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.map",
  node_version: 1,
  kind: "action",
  display_name: "Map",
  ports: {
    inputs: [{ name: "main", display_name: "Main" }],
    outputs: [
      { name: "main", display_name: "Main" },
      { name: "error", display_name: "Error" }
    ]
  },
  one_of: [{ params: ["body", "expression"], mode: "exactly" }],
  fields: [
    param("items", {
      label: "Items",
      type: "string",
      required: true,
      widget: "expression",
      help: "Expression that evaluates to the array to iterate",
      rules: [expression]
    }),
    param("batch_size", { label: "Batch Size", type: "number", default: 1, help: "Number of items processed per batch" }),
    param("continue_on_error", { label: "Continue On Error", type: "boolean", default: false, help: "Continue iteration when an item fails" }),
    param("body_concurrency", {
      label: "Body Concurrency",
      type: "number",
      default: 1,
      help: "Maximum items of one batch running at the same time; 1 (the default) runs them serially"
    }),
    param("body", {
      label: "Body",
      type: "object",
      help: "Sub-graph executed once per item; mutually exclusive with expression, and exactly one of the two is required"
    }),
    param("expression", {
      label: "Expression",
      type: "string",
      widget: "expression",
      help: "Expression evaluated once per item over $item/$index/$items; mutually exclusive with body, and exactly one of the two is required",
      rules: [expression]
    })
  ]
};

const jsRuntimes = [
  { value: "goja", label: "goja", description: "Fastest cold start; cannot interrupt tight loops" },
  { value: "qjs", label: "QuickJS", description: "Slower first load; genuine mid-execution termination" }
];
const wasmRuntimes = [
  { value: "wazero", label: "wazero" },
  { value: "wazero-reactor", label: "wazero (reactor)", description: "Pooled reactor-mode guest instances" }
];

export const scriptSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.script",
  node_version: 1,
  kind: "action",
  display_name: "Script",
  ports: {
    inputs: [{ name: "main", display_name: "Main" }],
    outputs: [
      { name: "main", display_name: "Main" },
      { name: "error", display_name: "Error" }
    ]
  },
  groups: [
    { key: "source", display_name: "Source" },
    { key: "environment", display_name: "Environment" }
  ],
  one_of: [{ params: ["code", "artifact_digest", "__artifact_file_path"], mode: "exactly" }],
  fields: [
    param("language", {
      label: "Language",
      type: "string",
      required: true,
      group: "source",
      help: "Language family: js | wasm (no default — choose explicitly)",
      options: [
        { value: "js", label: "JavaScript" },
        { value: "wasm", label: "WebAssembly", description: "Base64 wasip1 module" }
      ]
    }),
    param("runtime", {
      label: "Runtime",
      type: "string",
      required: true,
      group: "source",
      help: "Engine: js->goja|qjs, wasm->wazero|wazero-reactor (no default — choose explicitly)",
      options: [...jsRuntimes, ...wasmRuntimes],
      options_when: [
        { when: eq("language", "js"), options: jsRuntimes },
        { when: eq("language", "wasm"), options: wasmRuntimes }
      ]
    }),
    param("code", {
      label: "Code",
      type: "string",
      group: "source",
      widget: "code",
      help: "JS source (js) or base64 wasm module (wasm); omit when artifact_digest is set"
    }),
    param("artifact_digest", {
      label: "Artifact Digest",
      type: "string",
      group: "source",
      help: "Content-addressable digest (sha256:<hex>) of the script in the artifact store",
      rules: [{ type: "format", format: "sha256-digest" }]
    }),
    param("__artifact_file_path", {
      label: "Artifact File Path",
      type: "string",
      group: "source",
      help: "SDK build-time placeholder for Script.File(); rewritten to artifact_digest before the workflow runs and never reaches Execute",
      visible_when: never
    }),
    param("credentials", {
      label: "Credentials",
      type: "array",
      group: "environment",
      item: stringItem,
      widget: "credential-select",
      help: "Declared credential names injected as $credentials"
    }),
    param("roots", {
      label: "Roots",
      type: "array",
      group: "environment",
      item: stringItem,
      help: "Expression roots the script reads; omit to send the whole environment"
    })
  ]
};

export const httpSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.http",
  node_version: 1,
  kind: "action",
  display_name: "HTTP Request",
  credentials: ["http_auth"],
  ports: {
    inputs: [{ name: "main", display_name: "Main" }],
    outputs: [
      { name: "main", display_name: "Main" },
      { name: "error", display_name: "Error" }
    ]
  },
  groups: [
    { key: "request", display_name: "Request" },
    { key: "advanced", display_name: "Advanced", collapsed: true }
  ],
  fields: [
    param("method", {
      label: "Method",
      type: "string",
      default: "GET",
      group: "request",
      help: "HTTP method: GET/POST/PUT/DELETE/PATCH",
      options: ["GET", "POST", "PUT", "DELETE", "PATCH"].map((value) => ({ value }))
    }),
    param("url", {
      label: "URL",
      type: "string",
      required: true,
      group: "request",
      help: "Target URL",
      rules: [{ type: "format", format: "url", advisory: true }]
    }),
    param("mode", {
      label: "Mode",
      type: "string",
      group: "request",
      help: 'Payload encoding: "json" (default, body is marshalled and the response parsed) or "raw" (body in/out as opaque base64 bytes, every status is a result)',
      options: [
        { value: "json", label: "JSON", description: "Body marshalled to JSON; response parsed; 4xx is a node error" },
        { value: "raw", label: "Raw", description: "Body in and out as base64 bytes; every status is a result" }
      ]
    }),
    param("authentication", {
      label: "Authentication",
      type: "string",
      group: "request",
      widget: "credential-select",
      help: "Credential reference name"
    }),
    param("body", {
      label: "Body",
      type: "object",
      group: "request",
      help: "Request body, marshalled to JSON. Ignored when mode=raw",
      visible_when: inList("mode", "json", null)
    }),
    param("body_b64", {
      label: "Raw Body (base64)",
      type: "string",
      group: "request",
      widget: "base64",
      help: "Request body as base64-encoded bytes, sent verbatim with no Content-Type added. Only read when mode=raw",
      visible_when: eq("mode", "raw")
    }),
    param("headers", { label: "Headers", type: "object", group: "request", widget: "key-value", help: "Request headers" }),
    param("query", { label: "Query Params", type: "object", group: "request", widget: "key-value", help: "URL query parameters" }),
    param("options", {
      label: "Options",
      type: "object",
      group: "advanced",
      help: "Additional options (timeout, max_response_bytes, disable_redirect, insecure_skip_verify)"
    })
  ]
};

export const databaseSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.database",
  node_version: 1,
  kind: "action",
  display_name: "Database",
  credentials: ["db_conn"],
  ports: {
    inputs: [{ name: "main", display_name: "Main" }],
    outputs: [
      { name: "main", display_name: "Main" },
      { name: "error", display_name: "Error" }
    ]
  },
  fields: [
    param("operation", {
      label: "Operation",
      type: "string",
      required: true,
      help: 'DB operation: "select"/"insert"/"update"/"delete"/"insert_many"',
      options: ["select", "insert", "update", "delete", "insert_many"].map((value) => ({ value }))
    }),
    param("table", {
      label: "Table",
      type: "string",
      required: true,
      help: "Target table name",
      rules: [{ type: "pattern", pattern: "^[A-Za-z_][A-Za-z0-9_]*$" }]
    }),
    param("credential", {
      label: "Credential",
      type: "string",
      required: true,
      widget: "credential-select",
      help: "Credential reference name for the DB connection"
    }),
    param("where", {
      label: "Where",
      type: "object",
      help: "Filter conditions (key-value pairs)",
      visible_when: inList("operation", "select", "update", "delete"),
      required_when: inList("operation", "update", "delete")
    }),
    param("data", {
      label: "Data",
      type: "object",
      help: "Row data for insert/update operations; an array of row objects for insert_many",
      visible_when: inList("operation", "insert", "insert_many", "update"),
      required_when: inList("operation", "insert", "insert_many", "update")
    }),
    param("columns", {
      label: "Columns",
      type: "array",
      item: stringItem,
      help: "Columns to select (default: all)",
      visible_when: eq("operation", "select")
    }),
    param("limit", { label: "Limit", type: "number", help: "Max rows to return for select", visible_when: eq("operation", "select") })
  ]
};

const kafkaAggregate = "/parameters/aggregate";
const kafkaSchemaPath = "/parameters/message_schema";
const kafkaTuning = "/parameters/tuning";

export const kafkaSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.trigger.kafka",
  node_version: 1,
  kind: "trigger",
  display_name: "Kafka Trigger",
  ports: { outputs: [{ name: "main", display_name: "Main" }] },
  groups: [
    { key: "connection", display_name: "Connection" },
    { key: "processing", display_name: "Processing" },
    { key: "advanced", display_name: "Advanced", collapsed: true }
  ],
  fields: [
    param("brokers", { label: "Brokers", type: "array", required: true, group: "connection", item: stringItem }),
    param("topic", { label: "Topic", type: "string", required: true, group: "connection" }),
    param("group", { label: "Group", type: "string", required: true, group: "connection" }),
    param("start_offset", {
      label: "Start Offset",
      type: "string",
      default: "latest",
      group: "connection",
      options: [
        { value: "latest", label: "Latest", description: "Only records produced after the group first joins" },
        { value: "earliest", label: "Earliest", description: "From the oldest retained record" },
        { value: "last", label: "Last", description: "Alias of latest" },
        { value: "newest", label: "Newest", description: "Alias of latest" },
        { value: "first", label: "First", description: "Alias of earliest" },
        { value: "oldest", label: "Oldest", description: "Alias of earliest" },
        { value: "beginning", label: "Beginning", description: "Alias of earliest" }
      ]
    }),
    param("max_inflight", { label: "Max Inflight", type: "number", default: 64, group: "processing" }),
    param("aggregate", {
      label: "Aggregate",
      type: "object",
      group: "processing",
      help: "Optional partition batch aggregation (abridged; see kafka.go)",
      fields: [
        sub(kafkaAggregate, "enabled", {
          label: "Enabled",
          type: "boolean",
          help: "Aggregation is off unless this is true; every other field is ignored while it is off"
        }),
        sub(kafkaAggregate, "by", {
          label: "By",
          type: "string",
          help: 'Batch key; only "partition" is supported (the default)',
          options: [{ value: "partition" }]
        }),
        sub(kafkaAggregate, "max_size", { label: "Max Size", type: "number", help: "Records per batch; unset or non-positive means 100" }),
        sub(kafkaAggregate, "flush_interval", {
          label: "Flush Interval",
          type: "string",
          widget: "duration",
          help: "Positive duration; defaults to 1s in entry-seed mode and 100ms otherwise",
          rules: [duration]
        }),
        sub(kafkaAggregate, "dedup", {
          label: "Dedup",
          type: "string",
          help: 'Deduplication unit; only "message" is supported (the default)',
          options: [{ value: "message" }]
        }),
        sub(kafkaAggregate, "on_overflow", {
          label: "On Overflow",
          type: "string",
          help: "Policy when a partition reaches its retained bound; unset means discard",
          options: [
            { value: "discard", label: "Discard", description: "Drop the arriving record (counted and logged)" },
            { value: "block", label: "Block", description: "Halt fetching for the whole assignment until the partition drains" },
            { value: "dead_letter", label: "Dead Letter", description: "Republish to dead_letter_topic before advancing the offset" }
          ]
        }),
        sub(kafkaAggregate, "dead_letter_topic", {
          label: "Dead Letter Topic",
          type: "string",
          help: "Overflow topic; read only when on_overflow is dead_letter",
          visible_when: eq("on_overflow", "dead_letter"),
          required_when: eq("on_overflow", "dead_letter")
        })
      ]
    }),
    param("message_schema", {
      label: "Message Schema",
      type: "object",
      group: "processing",
      help: "Optional message validation (abridged; see kafka.go)",
      fields: [
        sub(kafkaSchemaPath, "required_fields", {
          label: "Required Fields",
          type: "array",
          item: stringItem,
          help: "Top-level keys every message value must carry; an empty list disables validation"
        }),
        sub(kafkaSchemaPath, "on_invalid", {
          label: "On Invalid",
          type: "string",
          help: "Policy for an invalid message; unset means discard",
          options: [
            { value: "discard", label: "Discard", description: "Commit the offset and drop the message (counted and logged)" },
            { value: "fail", label: "Fail", description: "Withhold the commit so Kafka redelivers; blocks the partition" },
            { value: "dead_letter", label: "Dead Letter", description: "Republish to dead_letter_topic, then commit" }
          ]
        }),
        sub(kafkaSchemaPath, "dead_letter_topic", {
          label: "Dead Letter Topic",
          type: "string",
          help: "Read only when on_invalid is dead_letter",
          visible_when: eq("on_invalid", "dead_letter"),
          required_when: eq("on_invalid", "dead_letter")
        })
      ]
    }),
    param("tuning", {
      label: "Tuning",
      type: "object",
      group: "advanced",
      help: "Optional consumer tuning (abridged; see kafka.go)",
      fields: [
        sub(kafkaTuning, "fetch_min_bytes", {
          label: "Fetch Min Bytes",
          type: "number",
          help: "Default 1; must not exceed fetch_max_bytes",
          rules: [{ type: "min", value: 1 }]
        }),
        sub(kafkaTuning, "fetch_max_bytes", { label: "Fetch Max Bytes", type: "number", help: "Default 10000000", rules: [{ type: "min", value: 1 }] }),
        ...(["max_wait", "dial_timeout", "session_timeout", "heartbeat_interval", "rebalance_timeout"] as const).map((name) =>
          sub(kafkaTuning, name, {
            label: name
              .split("_")
              .map((word) => word[0].toUpperCase() + word.slice(1))
              .join(" "),
            type: "string",
            widget: "duration",
            rules: [duration]
          })
        )
      ]
    })
  ]
};

const redisTuning = "/parameters/tuning";

export const redisSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.trigger.redis",
  node_version: 1,
  kind: "trigger",
  display_name: "Redis Trigger",
  ports: { outputs: [{ name: "main", display_name: "Main" }] },
  groups: [
    { key: "connection", display_name: "Connection" },
    { key: "advanced", display_name: "Advanced", collapsed: true }
  ],
  fields: [
    param("addr", {
      label: "Address",
      type: "string",
      required: true,
      group: "connection",
      help: "Redis server address, host:port. Credentials are not params: attach a supply with username/password to the node.",
      rules: [{ type: "format", format: "host-port", advisory: true }]
    }),
    param("mode", {
      label: "Mode",
      type: "string",
      default: "stream",
      group: "connection",
      options: [
        { value: "stream", label: "Stream", description: "Consume a Redis Stream through a consumer group" },
        { value: "pubsub", label: "Pub/Sub", description: "Subscribe to a Pub/Sub channel (one active replica)" }
      ]
    }),
    param("stream", {
      label: "Stream",
      type: "string",
      group: "connection",
      visible_when: inList("mode", "stream", null),
      required_when: inList("mode", "stream", null)
    }),
    param("group", {
      label: "Group",
      type: "string",
      group: "connection",
      visible_when: inList("mode", "stream", null),
      required_when: inList("mode", "stream", null)
    }),
    param("channel", {
      label: "Channel",
      type: "string",
      group: "connection",
      visible_when: eq("mode", "pubsub"),
      required_when: eq("mode", "pubsub")
    }),
    param("max_inflight", { label: "Max Inflight", type: "number", default: 64, group: "advanced" }),
    param("tuning", {
      label: "Tuning",
      type: "object",
      group: "advanced",
      help: "Stream-mode knobs (abridged; see redis.go)",
      fields: [
        sub(redisTuning, "db", { label: "DB", type: "number", help: "Database index; default 0" }),
        sub(redisTuning, "consumer", {
          label: "Consumer",
          type: "string",
          help: "Consumer name inside the group (stream mode); defaults to the node name"
        }),
        sub(redisTuning, "start_id", { label: "Start ID", type: "string", help: 'Group start ID (stream mode); default "$"' }),
        sub(redisTuning, "payload_field", {
          label: "Payload Field",
          type: "string",
          help: "Entry field used as the payload (stream mode); unset JSON-encodes the whole entry"
        }),
        sub(redisTuning, "claim_min_idle", {
          label: "Claim Min Idle",
          type: "string",
          widget: "duration",
          help: "Positive duration (stream mode); default 1m",
          rules: [duration]
        }),
        sub(redisTuning, "dial_timeout", {
          label: "Dial Timeout",
          type: "string",
          widget: "duration",
          help: "Positive duration; default 10s",
          rules: [duration]
        })
      ]
    })
  ]
};

export const aggregateSchema: NodeFormSchema = {
  spec: "node-form/v1",
  node_type: "xflow.transform.aggregate",
  node_version: 1,
  kind: "action",
  display_name: "Aggregate",
  ports: { inputs: [{ name: "main", display_name: "Main" }], outputs: [{ name: "main", display_name: "Main" }] },
  fields: [
    param("items", {
      label: "Items",
      type: "string",
      required: true,
      widget: "expression",
      help: "Expression that evaluates to the array to aggregate",
      rules: [expression]
    }),
    param("operations", {
      label: "Operations",
      type: "array",
      required: true,
      help: "Aggregate operations",
      item: {
        type: "object",
        fields: [
          itemField("kind", {
            label: "Kind",
            type: "string",
            required: true,
            options: [
              { value: "count", label: "Count" },
              { value: "sum", label: "Sum" },
              { value: "avg", label: "Average" },
              { value: "average", label: "Average (alias of avg)" }
            ]
          }),
          itemField("field", {
            label: "Field",
            type: "string",
            help: "Item field to sum or average",
            visible_when: inList("kind", "sum", "avg", "average")
          }),
          itemField("as", { label: "As", type: "string", required: true, help: "Output field name" })
        ]
      }
    })
  ]
};

export const fixtureSchemas: readonly NodeFormSchema[] = [
  waitSchema,
  switchSchema,
  mapSchema,
  scriptSchema,
  httpSchema,
  databaseSchema,
  kafkaSchema,
  redisSchema,
  aggregateSchema
];
