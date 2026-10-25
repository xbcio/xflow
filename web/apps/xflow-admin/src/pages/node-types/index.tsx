import {
  BranchIcon,
  CodeIcon,
  CopyIcon,
  FilterIcon,
  FlagIcon,
  GlobeIcon,
  GridIcon,
  SearchIcon,
  UserIcon,
} from '@xflow/icons';
import { createXFlowApiClient } from '@xflow/api';
import {
  categoryOf,
  nodeCategoryMeta,
  type CategoryIconKey,
  type NodeCategory,
  type NodeFormExample,
  type NodeFormSchema,
  type ParamValidationMode,
} from '@xflow/core';
import { Alert, Button, Input, Select, Table, type InputRef, type TableColumnsType } from 'antd';
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';

import './node-types.css';

import { PageHeader } from '../../components/PageHeader';
import { TableProgressBar } from '../../components/TableProgressBar';
import { TableToolbar } from '../../components/TableToolbar';
import { useSearchShortcut } from '../../components/useSearchShortcut';
import { authorizedFetch } from '../../services/http';
import { PanelTitle, TableSkeleton } from '../workflows/parts';

/** Category glyph, matching the editor's node-library taxonomy (spec §5). */
const categoryIcon: Record<CategoryIconKey, ReactNode> = {
  flag: <FlagIcon />,
  branch: <BranchIcon />,
  globe: <GlobeIcon />,
  code: <CodeIcon />,
  filter: <FilterIcon />,
  user: <UserIcon />,
  grid: <GridIcon />,
};

/** Deterministic filter order, from the taxonomy rather than response order. */
const CATEGORY_ORDER: NodeCategory[] = [
  'trigger',
  'control',
  'action',
  'code',
  'transform',
  'human',
  'unknown',
];

type CategoryFilter = 'all' | NodeCategory;

/** XFlowApiError carries `status`; tolerate a umi-request-shaped rejection too. */
function statusOf(reason: unknown): number | undefined {
  const direct = (reason as { status?: unknown } | null)?.status;
  if (typeof direct === 'number') return direct;
  const response = (reason as { response?: { status?: unknown } } | null)?.response?.status;
  return typeof response === 'number' ? response : undefined;
}

function describeNodeTypesError(reason: unknown): string {
  const status = statusOf(reason);
  if (status === 403) {
    return '服务端返回 403：当前凭据缺少读取节点库所需的 workflow.read scope（workflow scope 同样有效）。服务端仍独立鉴权，前端不会绕过这一限制。';
  }
  if (status === 404) {
    return '服务端未提供 /v1/node-types（404）。请确认服务端版本包含节点类型接口。';
  }
  if (reason instanceof Error && reason.message) return reason.message;
  return '节点类型加载失败，请稍后重试。';
}

/** First line of the long-form docs, without a markdown heading marker. */
function descriptionOf(schema: NodeFormSchema): string {
  const text = (schema.description ?? schema.docs ?? '').trim();
  if (!text) return '';
  return text.split(/\r?\n/, 1)[0].replace(/^#+\s*/, '');
}

/** Examples the server declared for this type; absent means none. */
function examplesOf(schema: NodeFormSchema): NodeFormExample[] {
  return Array.isArray(schema.examples) ? schema.examples : [];
}

/** One param value as text: strings stay verbatim, structured values stay JSON. */
function exampleValueText(value: unknown): string {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

function exampleJson(example: NodeFormExample): string {
  return JSON.stringify(example.params ?? {}, null, 2);
}

type CopyState = 'idle' | 'copied' | 'failed';

/** Copies one example's parameter set as prettified JSON, with inline feedback. */
function CopyExampleButton({ example, label }: { example: NodeFormExample; label: string }) {
  const [state, setState] = useState<CopyState>('idle');
  const onCopy = () => {
    const clipboard = typeof navigator === 'undefined' ? undefined : navigator.clipboard;
    if (!clipboard?.writeText) {
      setState('failed');
      return;
    }
    clipboard.writeText(exampleJson(example)).then(
      () => setState('copied'),
      () => setState('failed'),
    );
  };
  return (
    <Button
      aria-label={`复制示例 ${label} 的 JSON`}
      className="xflow-node-types-example__copy"
      icon={<CopyIcon />}
      size="small"
      type="text"
      onClick={onCopy}
    >
      {state === 'copied' ? '已复制' : state === 'failed' ? '复制失败' : '复制 JSON'}
    </Button>
  );
}

/** Expanded row: every declared example with its params as monospace key/value pairs. */
function NodeExamples({ schema }: { schema: NodeFormSchema }) {
  const examples = examplesOf(schema);
  if (examples.length === 0) return null;
  return (
    <div className="xflow-node-types-examples">
      {examples.map((example, index) => {
        const label = example.name?.trim() || `示例 ${index + 1}`;
        const params = example.params ?? {};
        return (
          <section
            className="xflow-node-types-example"
            data-example={label}
            key={`${example.name ?? ''}-${index}`}
          >
            <header className="xflow-node-types-example__head">
              <span className="xflow-node-types-example__name">{label}</span>
              {example.description ? (
                <span className="xflow-node-types-example__desc">{example.description}</span>
              ) : null}
              <CopyExampleButton example={example} label={label} />
            </header>
            {Object.keys(params).length === 0 ? (
              <span className="xflow-admin-faint">—</span>
            ) : (
              <dl className="xflow-node-types-params">
                {Object.entries(params).map(([key, value]) => (
                  <div className="xflow-node-types-param" key={key}>
                    <dt>{key}</dt>
                    <dd>{exampleValueText(value)}</dd>
                  </div>
                ))}
              </dl>
            )}
          </section>
        );
      })}
    </div>
  );
}

/**
 * 节点库 (LS-01). A read-only projection of `GET /v1/node-types`: the server
 * registry's form schemas plus any live runner-reported types in the caller's
 * namespace. There is deliberately no edit entry — descriptors are registered
 * by the server / runners, never by the console.
 *
 * Filtering is client-side because the response is the complete registry and
 * is small; the endpoint has no filter or pagination parameters.
 */
export default function NodeTypesPage() {
  // `@xflow/api` defaults to the global fetch, which does not carry the admin
  // session's Bearer token; authorizedFetch is what makes this read
  // authenticate like the umi request services.
  const api = useMemo(
    () => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }),
    [],
  );

  const [schemas, setSchemas] = useState<NodeFormSchema[]>([]);
  const [validationMode, setValidationMode] = useState<ParamValidationMode>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [attempt, setAttempt] = useState(0);
  const [category, setCategory] = useState<CategoryFilter>('all');
  const [query, setQuery] = useState('');
  // `/` focuses this page's own search; the shell's top bar carries global
  // chrome only (提案 C).
  const searchRef = useRef<InputRef>(null);
  useSearchShortcut(searchRef);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(undefined);
    api.listNodeTypes().then(
      (result) => {
        if (cancelled) return;
        setSchemas(result.node_types);
        setValidationMode(result.param_validation_mode);
        setLoading(false);
      },
      (reason: unknown) => {
        if (cancelled) return;
        setError(describeNodeTypesError(reason));
        setLoading(false);
      },
    );
    return () => {
      cancelled = true;
    };
  }, [api, attempt]);

  const reload = () => setAttempt((value) => value + 1);

  const categories = useMemo(() => {
    const present = new Set<NodeCategory>();
    for (const schema of schemas) present.add(categoryOf(schema.node_type, schema.category));
    return CATEGORY_ORDER.filter((key) => present.has(key));
  }, [schemas]);

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return schemas.filter((schema) => {
      if (category !== 'all' && categoryOf(schema.node_type, schema.category) !== category) return false;
      if (!needle) return true;
      return (
        schema.node_type.toLowerCase().includes(needle) ||
        (schema.display_name ?? '').toLowerCase().includes(needle) ||
        descriptionOf(schema).toLowerCase().includes(needle)
      );
    });
  }, [schemas, category, query]);

  const columns: TableColumnsType<NodeFormSchema> = [
    {
      title: '名称 / 类型',
      key: 'name',
      width: 260,
      render: (_value: unknown, schema) => (
        <span className="xflow-admin-name">
          <strong title={schema.display_name || schema.node_type}>
            {schema.display_name || schema.node_type}
          </strong>
          <small title={schema.node_type}>
            {schema.node_type} · v{schema.node_version}
          </small>
        </span>
      ),
    },
    {
      title: '分类',
      key: 'category',
      width: 120,
      render: (_value: unknown, schema) => {
        const key = categoryOf(schema.node_type, schema.category);
        return (
          <span className="xflow-admin-chip" data-category={key}>
            <span className="xflow-admin-chip-icon" aria-hidden="true">
              {categoryIcon[nodeCategoryMeta[key].icon]}
            </span>
            <span>{nodeCategoryMeta[key].label}</span>
          </span>
        );
      },
    },
    {
      title: '描述',
      key: 'description',
      width: 380,
      render: (_value: unknown, schema) => {
        const text = descriptionOf(schema);
        return text ? (
          <span className="xflow-admin-node-desc" title={text}>
            {text}
          </span>
        ) : (
          <span className="xflow-admin-faint">—</span>
        );
      },
    },
    {
      title: '参数',
      key: 'params',
      width: 80,
      align: 'right',
      render: (_value: unknown, schema) => (
        <span className="xflow-admin-num" data-param-count={schema.fields.length}>
          {schema.fields.length}
        </span>
      ),
    },
  ];

  const firstLoad = loading && schemas.length === 0 && !error;
  const refresh = (
    <Button onClick={reload} disabled={loading}>
      刷新
    </Button>
  );

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <PageHeader
        title="节点库"
        description={
          <>
            服务端注册的节点类型与表单参数，只读。
            {validationMode ? ` 参数校验模式：${validationMode}。` : null}
          </>
        }
        actions={refresh}
      />

      {error ? (
        <Alert
          type="error"
          showIcon
          title="节点库加载失败"
          description={error}
          action={
            <Button size="small" onClick={reload}>
              重试
            </Button>
          }
        />
      ) : (
        <div className="xflow-admin-card xflow-admin-tablecard" data-table="node-types">
          <PanelTitle icon={<GridIcon />} title="节点类型" count={schemas.length} />
          <TableToolbar
            label="节点类型筛选"
            actions={
              <span className="xflow-admin-panel-count" data-filtered-count={filtered.length}>
                {filtered.length} / {schemas.length}
              </span>
            }
          >
            <Input
              ref={searchRef}
              className="xflow-admin-search"
              aria-label="搜索节点类型"
              placeholder="按名称、类型或描述搜索"
              prefix={<SearchIcon />}
              suffix={
                <kbd className="xflow-admin-kbd" aria-hidden="true">
                  /
                </kbd>
              }
              allowClear
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
            <Select<CategoryFilter>
              className="xflow-admin-filter-select"
              aria-label="按分类筛选"
              value={category}
              onChange={setCategory}
              popupMatchSelectWidth={false}
              options={[
                { value: 'all', label: '全部分类' },
                ...categories.map((key) => ({
                  value: key,
                  label: nodeCategoryMeta[key].label,
                })),
              ]}
            />
          </TableToolbar>
          <TableProgressBar loading={loading && !firstLoad} label="正在刷新节点类型" />
          <Table
            className="xflow-admin-table"
            size="small"
            rowKey={(schema) => `${schema.node_type}@${schema.node_version}`}
            tableLayout="fixed"
            scroll={{ x: 900 }}
            pagination={false}
            expandable={{
              rowExpandable: (schema) => examplesOf(schema).length > 0,
              expandedRowRender: (schema) => <NodeExamples schema={schema} />,
            }}
            locale={{
              emptyText: firstLoad ? (
                <TableSkeleton rows={5} density="two-line" label="正在加载节点类型" />
              ) : schemas.length === 0 ? (
                '暂无已注册的节点类型。'
              ) : (
                '没有匹配的节点类型。'
              ),
            }}
            dataSource={filtered}
            onRow={(schema) =>
              ({
                'data-node-type': `${schema.node_type}@${schema.node_version}`,
              }) as Record<string, string>
            }
            columns={columns}
          />
        </div>
      )}
    </div>
  );
}
