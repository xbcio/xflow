// NodeFormSchema v1 (Doc C §2). The wire types live in @xflow/core
// (xflow-core/src/nodeForm.ts) so @xflow/api can type GET /v1/node-types
// without depending on the editor; this module re-exports them so node-form
// imports keep working. See nodeForm.ts for the wire-format notes.

export type {
  Condition,
  ConditionalOptions,
  EnumOption,
  ExpressionModeName,
  FieldRule,
  FieldRuleType,
  FieldType,
  KnownWidget,
  NodeFormField,
  NodeFormGroup,
  NodeFormItem,
  NodeFormKind,
  NodeFormOneOf,
  NodeFormPort,
  NodeFormPorts,
  NodeFormSchema,
  NodeFormSource,
  NodeFormSpecVersion,
  NodeTypesResponse,
  ParamValidationMode,
  WidgetName
} from "@xflow/core";
