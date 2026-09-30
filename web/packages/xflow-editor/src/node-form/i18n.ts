// Node-form i18n (Doc C §8 item 1, option "frontend catalog"). The server
// projects Descriptor text as English literals; the editor translates it here,
// before anything reads the schemas, so the node library, ports, and forms all
// see one localized GET /v1/node-types response.
//
// The catalog is keyed by the English source text (gettext style), not by
// node type and path: a reworded English string no longer matches, so the
// fixture coverage test (i18n.test.ts) fails and the stale translation cannot
// survive silently. `byType` disambiguates the rare word that needs a
// different translation in one node type.
//
// Anything without an entry stays English. That is the expected outcome for
// custom node types: their Descriptors are authored elsewhere and only builtins
// are catalogued.

import type { EnumOption, NodeFormField, NodeFormItem, NodeFormPort, NodeFormSchema, NodeTypesResponse } from "./schema";

export interface NodeFormCatalog {
  /** BCP 47 tag of the translations, e.g. "zh-CN". */
  readonly locale: string;
  /** English source text → translation, shared by every node type. */
  readonly messages: Readonly<Record<string, string>>;
  /** node_type → English source text → translation; wins over `messages`. */
  readonly byType?: Readonly<Record<string, Readonly<Record<string, string>>>>;
  /**
   * Source strings deliberately left untranslated (product names, protocol
   * terms). Listing them lets the coverage test tell "kept on purpose" from
   * "forgotten".
   */
  readonly keep?: readonly string[];
}

/** Translate one source string for a node type; unknown text is returned as-is. */
export function translateNodeFormText(catalog: NodeFormCatalog, text: string, nodeType?: string): string {
  if (nodeType) {
    const scoped = catalog.byType?.[nodeType]?.[text];
    if (scoped !== undefined) return scoped;
  }
  return catalog.messages[text] ?? text;
}

type Translate = (text: string | undefined) => string | undefined;

function localizeOptions(options: EnumOption[] | null | undefined, t: Translate): EnumOption[] | null | undefined {
  if (!options) return options;
  return options.map((option) => ({
    ...option,
    ...(option.label !== undefined && { label: t(option.label) }),
    ...(option.description !== undefined && { description: t(option.description) })
  }));
}

function localizeField<F extends NodeFormField | NodeFormItem>(field: F, t: Translate): F {
  const out: F = { ...field };
  if (field.label !== undefined) out.label = t(field.label);
  if (field.help !== undefined) out.help = t(field.help);
  if (typeof field.deprecated === "string") out.deprecated = t(field.deprecated);
  if (field.rules) out.rules = field.rules.map((rule) => (rule.message !== undefined ? { ...rule, message: t(rule.message) } : rule));
  if (field.options) out.options = localizeOptions(field.options, t);
  if (field.options_when) out.options_when = field.options_when.map((entry) => ({ ...entry, options: localizeOptions(entry.options, t) ?? [] }));
  if (field.fields) out.fields = field.fields.map((child) => localizeField(child, t));
  if (field.item) out.item = localizeField(field.item, t);
  return out;
}

/** A localized copy of one schema; values, names, and paths are untouched. */
export function localizeNodeFormSchema(schema: NodeFormSchema, catalog: NodeFormCatalog): NodeFormSchema {
  const t: Translate = (text) => (text === undefined || text === "" ? text : translateNodeFormText(catalog, text, schema.node_type));
  const ports = schema.ports;
  const localizePorts = (list: NodeFormPort[]): NodeFormPort[] =>
    list.map((port) => (port.display_name ? { ...port, display_name: t(port.display_name) } : port));
  return {
    ...schema,
    ...(schema.display_name !== undefined && { display_name: t(schema.display_name) }),
    ...(schema.description !== undefined && { description: t(schema.description) }),
    ...(ports && {
      ports: {
        ...ports,
        ...(ports.inputs && { inputs: localizePorts(ports.inputs) }),
        ...(ports.outputs && { outputs: localizePorts(ports.outputs) })
      }
    }),
    ...(schema.groups && {
      groups: schema.groups.map((group) => ({
        ...group,
        ...(group.display_name !== undefined && { display_name: t(group.display_name) }),
        ...(group.description !== undefined && { description: t(group.description) })
      }))
    }),
    fields: Array.isArray(schema.fields) ? schema.fields.map((field) => localizeField(field, t)) : schema.fields
  };
}

const localized = new WeakMap<NodeFormCatalog, WeakMap<NodeTypesResponse, NodeTypesResponse>>();

/**
 * The localized response, memoised per (catalog, response) so the result keeps
 * its identity across renders: the node library and the form compiler both
 * cache by object identity.
 */
export function localizeNodeTypes<R extends NodeTypesResponse | undefined>(response: R, catalog: NodeFormCatalog): R {
  if (!response || !Array.isArray(response.node_types)) return response;
  let byResponse = localized.get(catalog);
  if (!byResponse) {
    byResponse = new WeakMap();
    localized.set(catalog, byResponse);
  }
  let out = byResponse.get(response);
  if (!out) {
    out = {
      ...response,
      // A malformed entry is passed through; the consumers already skip it.
      node_types: response.node_types.map((schema) => (schema && typeof schema === "object" ? localizeNodeFormSchema(schema, catalog) : schema))
    };
    byResponse.set(response, out);
  }
  return out as R;
}
