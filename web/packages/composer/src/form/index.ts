// @xflow/composer/form — built-in form components on AntD (Doc B §5, §9).
// Depends only on the composer component contract (composer/react) and
// core helpers; never on a kernel. Styles: `@xflow/composer/form/styles.css`.

import type { ComposerComponent, CreateRegistryOptions, Registry } from "../react";
import { createRegistry } from "../react";
import { ArrayTable } from "./ArrayTable";
import { InputNumber, MultiSelect, Radio, Select, Switch, Tags } from "./choice";
import { JsonEditor } from "./JsonEditor";
import { KeyValue } from "./KeyValue";
import { FieldGroup, Form, ObjectGroup } from "./layout";
import { ElementError, Unsupported } from "./placeholders";
import { CodeEditor, Input, Password, TextArea } from "./text";

export { ArrayTable, type ArrayTableProps } from "./ArrayTable";
export {
  InputNumber,
  MultiSelect,
  Radio,
  Select,
  Switch,
  Tags,
  type InputNumberProps,
  type RadioProps,
  type SelectProps,
  type SwitchProps,
  type TagsProps
} from "./choice";
export { JsonEditor, parseJsonText, type JsonEditorProps } from "./JsonEditor";
export { KeyValue, type KeyValueProps } from "./KeyValue";
export { FieldGroup, Form, ObjectGroup, type FieldGroupProps, type FormProps, type ObjectGroupProps } from "./layout";
export { ElementError, Unsupported } from "./placeholders";
export { FieldFrame, FieldLayoutContext, RawValue, type FieldFrameProps, type FieldIds, type FieldLayout, type FieldProps, type Option } from "./shared";
export { CodeEditor, Input, Password, TextArea, type CodeEditorProps, type InputProps, type TextAreaProps } from "./text";

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- heterogeneous prop types
export const formComponents: readonly ComposerComponent<any>[] = Object.freeze([
  Form,
  FieldGroup,
  Input,
  TextArea,
  Password,
  InputNumber,
  Switch,
  Select,
  Radio,
  MultiSelect,
  Tags,
  KeyValue,
  ObjectGroup,
  ArrayTable,
  JsonEditor,
  CodeEditor,
  Unsupported,
  ElementError
]);

/**
 * Registry of the built-in form components. `extra` components are
 * registered on top (a host component with a built-in's type replaces it,
 * logged at debug level); `options` go to that second createRegistry call.
 */
export function createFormRegistry(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- heterogeneous prop types
  extra: readonly ComposerComponent<any>[] = [],
  options: Omit<CreateRegistryOptions, "extends"> = {}
): Registry {
  const base = createRegistry(formComponents);
  if (extra.length === 0 && !options.checks?.length && !options.expressions?.length) return base;
  return createRegistry(extra, { ...options, extends: base });
}
