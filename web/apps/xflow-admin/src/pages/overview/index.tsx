import { createXFlowApiClient, type OverviewResponse, type OverviewWindow } from '@xflow/api';
import { InfoIcon, PulseIcon, ServerIcon, WarnIcon } from '@xflow/icons';
import { history } from '@umijs/max';
import { Alert, Button, Segmented, Skeleton } from 'antd';
import { useEffect, useMemo, useState } from 'react';

import './overview.css';

import { PageHeader } from '../../components/PageHeader';
import { authorizedFetch } from '../../services/http';
import { formatDuration, formatPercent, fullTimestamp, relativeTime } from '../workflows/row-parts';
import { PanelTitle } from '../workflows/parts';
import { failedSeries, Sparkline, StatusHistogram, type SparkTone } from './charts';
import {
  describeOverviewError,
  failedCount,
  failedDelta,
  histogramTotal,
  OVERVIEW_WINDOW_LABEL,
  runsDelta,
  type DeltaDirection,
  type OverviewDelta,
  type OverviewFailure,
} from './model';

/**
 * A-A overview dashboard: `GET /v1/overview` rendered as KPIs, a 24-bucket
 * status histogram, the five workflows with the most failures and — when the
 * caller also holds `management.runner.list` — the cluster segment.
 *
 * The page never invents a number. Every metric the schema marks nullable is
 * printed as "—" with a reason, the cluster segment is absent for an
 * unauthorized caller (the card explains the missing grant instead of failing),
 * and a deployment without an execution store gets its own 501 copy.
 */

interface KpiCardProps {
  id: string;
  label: string;
  value: string;
  hint?: string;
  tone?: 'danger';
  delta?: OverviewDelta;
  series?: Array<number | null>;
  seriesTone?: SparkTone;
  seriesLabel?: string;
}

function DeltaBadge({ delta }: { delta: OverviewDelta }) {
  const arrow: Record<DeltaDirection, string> = { up: '↑', down: '↓', flat: '→' };
  return (
    <span
      className="xflow-ov-delta"
      data-direction={delta.direction}
      data-semantics={delta.semantics}
      title={delta.title}
    >
      <span aria-hidden="true">{arrow[delta.direction]}</span>
      {delta.label}
    </span>
  );
}

function KpiCard({
  id,
  label,
  value,
  hint,
  tone,
  delta,
  series,
  seriesTone = 'primary',
  seriesLabel,
}: KpiCardProps) {
  return (
    <article className="xflow-admin-card xflow-ov-kpi" data-kpi={id}>
      <div className="xflow-ov-kpi-label">{label}</div>
      <div className="xflow-ov-kpi-main">
        <span className="xflow-ov-kpi-value" data-tone={tone}>
          {value}
        </span>
        {delta ? <DeltaBadge delta={delta} /> : null}
        {series ? <Sparkline values={series} tone={seriesTone} label={seriesLabel ?? label} /> : null}
      </div>
      {hint ? <div className="xflow-ov-kpi-hint">{hint}</div> : null}
    </article>
  );
}

function OverviewSkeleton() {
  return (
    <div className="xflow-ov-skeleton" role="status" aria-label="正在加载概览">
      <div className="xflow-ov-kpis">
        {Array.from({ length: 6 }, (_value, index) => (
          <div className="xflow-admin-card xflow-ov-kpi" key={index}>
            <Skeleton.Input active block size="small" />
          </div>
        ))}
      </div>
      <div className="xflow-ov-middle">
        <div className="xflow-admin-card">
          <div className="xflow-ov-skel-body">
            <Skeleton.Input active block style={{ height: 200 }} />
          </div>
        </div>
        <div className="xflow-admin-card">
          <div className="xflow-ov-skel-body">
            {Array.from({ length: 5 }, (_value, index) => (
              <Skeleton.Input active block size="small" key={index} />
            ))}
          </div>
        </div>
      </div>
      <div className="xflow-admin-card">
        <div className="xflow-ov-skel-body">
          <Skeleton.Input active block size="small" />
        </div>
      </div>
    </div>
  );
}

function ClusterCard({ cluster }: { cluster: NonNullable<OverviewResponse['cluster']> }) {
  return (
    <section className="xflow-admin-card xflow-ov-cluster" data-cluster="available">
      <PanelTitle icon={<ServerIcon />} title="集群" />
      <div className="xflow-ov-cluster-body">
        <div className="xflow-ov-cluster-grid">
          <div className="xflow-ov-cluster-item">
            <span className="xflow-ov-cluster-label">在线 Runner</span>
            <span className="xflow-ov-cluster-value">
              {cluster.runners.online} / {cluster.runners.total}
            </span>
          </div>
          <div className="xflow-ov-cluster-item">
            <span className="xflow-ov-cluster-label">总容量</span>
            <span className="xflow-ov-cluster-value">{cluster.runners.capacity}</span>
          </div>
          <div className="xflow-ov-cluster-item">
            <span className="xflow-ov-cluster-label">在途任务</span>
            <span className="xflow-ov-cluster-value">{cluster.runners.inFlight}</span>
          </div>
          <div className="xflow-ov-cluster-item">
            <span className="xflow-ov-cluster-label">Leader</span>
            <span className="xflow-ov-cluster-value">
              {cluster.isLeader ? '本节点' : '其他节点'}
            </span>
          </div>
        </div>
        <div
          className="xflow-ov-meter"
          role="progressbar"
          aria-label="在途任务占在线容量的比例"
          aria-valuemin={0}
          aria-valuemax={Math.max(cluster.runners.capacity, 1)}
          aria-valuenow={cluster.runners.inFlight}
        >
          <span
            className="xflow-ov-meter-fill"
            style={{
              width: `${
                cluster.runners.capacity > 0
                  ? Math.min(100, (cluster.runners.inFlight / cluster.runners.capacity) * 100)
                  : 0
              }%`,
            }}
          />
        </div>
        <p className="xflow-ov-meter-label">
          在途 {cluster.runners.inFlight} / 容量 {cluster.runners.capacity}
          {cluster.runners.capacity > 0
            ? `（${Math.round((cluster.runners.inFlight / cluster.runners.capacity) * 100)}%）`
            : ''}
        </p>
      </div>
    </section>
  );
}

function TopFailingCard({ rows }: { rows: OverviewResponse['topFailing'] }) {
  return (
    <section className="xflow-admin-card xflow-ov-top" aria-label="失败最多的工作流">
      <PanelTitle icon={<WarnIcon />} title="失败最多的 5 个工作流" />
      {rows.length === 0 ? (
        <p className="xflow-ov-empty">窗口内没有失败执行。</p>
      ) : (
        <div className="xflow-ov-fails">
          {rows.map((row, index) => {
            const name = row.workflowName ?? row.workflowId.slice(0, 8);
            return (
              <button
                type="button"
                className="xflow-ov-fail"
                data-workflow={row.workflowId}
                key={row.workflowId}
                title={row.lastError ?? row.workflowId}
                onClick={() => history.push(`/workflows/${encodeURIComponent(row.workflowId)}`)}
              >
                <span className="xflow-ov-fail-rank" aria-hidden="true">
                  {index + 1}
                </span>
                <span className="xflow-ov-fail-name">{name}</span>
                <span className="xflow-ov-fail-count">失败 {row.failed} 次</span>
                <span className="xflow-ov-fail-time">
                  {row.lastFailedAt ? `最近失败 ${relativeTime(row.lastFailedAt)}` : '—'}
                </span>
              </button>
            );
          })}
        </div>
      )}
    </section>
  );
}

export default function OverviewPage() {
  const [range, setRange] = useState<OverviewWindow>('24h');
  const [attempt, setAttempt] = useState(0);
  const [overview, setOverview] = useState<OverviewResponse>();
  const [failure, setFailure] = useState<OverviewFailure>();
  const [loading, setLoading] = useState(true);

  // admin session's Bearer token: the page builds the client directly, so the
  // same `authorizedFetch` the workflow centre uses must be handed to it.
  const client = useMemo(() => createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch }), []);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setFailure(undefined);
    // A window switch must not leave the previous window's numbers under the
    // new label: drop them and draw the fixed-height skeleton instead.
    setOverview(undefined);
    client
      .getOverview({ window: range })
      .then((next) => {
        if (!active) return;
        setOverview(next);
        setLoading(false);
      })
      .catch((reason: unknown) => {
        if (!active) return;
        setFailure(describeOverviewError(reason));
        setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [attempt, client, range]);

  const totals = overview
    ? {
        runs: histogramTotal(overview.histogram),
        failed: overview.histogram.reduce((sum, bucket) => sum + failedCount(bucket.counts), 0),
      }
    : undefined;

  const refresh = () => setAttempt((current) => current + 1);
  // model.ts words 501/403/401 itself in Chinese; every other failure carries
  // the server's own text, which must not be drawn as copy.
  const knownCopy = failure?.unsupported || failure?.status === 403 || failure?.status === 401;

  return (
    <div className="xflow-ov">
      <PageHeader
        title="概览"
        description="执行健康度、失败分布与集群容量的近实时视图。"
        actions={
          <>
            <Segmented
              aria-label="统计窗口"
              size="small"
              value={range}
              options={[
                { label: '24 小时', value: '24h' },
                { label: '7 天', value: '7d' },
              ]}
              onChange={(value) => setRange(value as OverviewWindow)}
            />
            <Button size="small" onClick={refresh} disabled={loading} data-action="refresh">
              刷新
            </Button>
          </>
        }
      />

      {failure ? (
        <Alert
          type="error"
          showIcon
          title={failure.message}
          // The 501/403/401 details are fixed Chinese copy; any other detail is
          // the server's own text and stays in the attribute with the status.
          // The leading space keeps headline and hint apart for innerText.
          description={<> {knownCopy ? failure.detail : '请稍后重试。'}</>}
          data-error-detail={[
            failure.status ? `HTTP ${failure.status}` : undefined,
            knownCopy ? undefined : failure.detail,
          ]
            .filter(Boolean)
            .join(' · ')}
          action={
            <Button size="small" onClick={refresh}>
              重试
            </Button>
          }
        />
      ) : null}

      {overview && totals ? (
        <>
          <section className="xflow-ov-kpis" aria-label="关键指标">
            <KpiCard
              id="runs"
              label="运行数"
              value={overview.kpis.runs.toLocaleString('zh-CN')}
              delta={runsDelta(overview.kpis.runs, overview.kpis.runsPrevious)}
              series={overview.kpis.sparkline.runs}
              seriesLabel={`${OVERVIEW_WINDOW_LABEL[overview.window]}运行数趋势`}
            />
            <KpiCard
              id="success-rate"
              label="成功率"
              value={
                overview.kpis.successRate === null ? '—' : formatPercent(overview.kpis.successRate)
              }
              hint={overview.kpis.successRate === null ? '窗口内没有终态执行' : undefined}
              series={overview.kpis.sparkline.successRate}
              seriesTone="success"
              seriesLabel={`${OVERVIEW_WINDOW_LABEL[overview.window]}成功率趋势`}
            />
            <KpiCard
              id="failed"
              label="失败数"
              value={totals.failed.toLocaleString('zh-CN')}
              hint="失败 + 超时"
              tone={totals.failed > 0 ? 'danger' : undefined}
              delta={failedDelta(totals.failed, overview.kpis.failedPrevious)}
              series={failedSeries(overview.histogram)}
              seriesTone="danger"
              seriesLabel={`${OVERVIEW_WINDOW_LABEL[overview.window]}失败数趋势`}
            />
            <KpiCard
              id="p50"
              label="P50 耗时"
              value={overview.kpis.p50Ms === null ? '—' : formatDuration(overview.kpis.p50Ms)}
              hint={overview.kpis.p50Ms === null ? '窗口内没有终态执行' : '终态执行的中位耗时'}
            />
            <KpiCard
              id="p95"
              label="P95 耗时"
              value={overview.kpis.p95Ms === null ? '—' : formatDuration(overview.kpis.p95Ms)}
              hint={overview.kpis.p95Ms === null ? '窗口内没有终态执行' : '终态执行的尾部耗时'}
            />
            <KpiCard
              id="active-workflows"
              label="活跃工作流"
              value={overview.kpis.activeWorkflows.toLocaleString('zh-CN')}
              hint={`${OVERVIEW_WINDOW_LABEL[overview.window]}内有执行的去重工作流`}
            />
          </section>

          <div className="xflow-ov-middle">
            <section className="xflow-admin-card xflow-ov-chart" aria-label="执行状态分布">
              <PanelTitle icon={<PulseIcon />} title="执行状态分布" count={totals.runs} />
              <div className="xflow-ov-chart-body">
                <StatusHistogram histogram={overview.histogram} range={overview.window} />
              </div>
            </section>
            <TopFailingCard rows={overview.topFailing} />
          </div>

          {overview.cluster ? (
            <ClusterCard cluster={overview.cluster} />
          ) : (
            // The whole segment is absent for a caller without the roster
            // grant; a secondary line says why instead of an empty card.
            <p className="xflow-ov-note xflow-ov-cluster-missing" data-cluster="unavailable">
              <InfoIcon />
              需要 management.runner.list 权限才能查看集群状态。
            </p>
          )}

          <p className="xflow-ov-foot">
            数据生成于 {fullTimestamp(overview.generatedAt)}；服务端最多缓存 30 秒。
          </p>
        </>
      ) : null}

      {!overview && !failure ? <OverviewSkeleton /> : null}
    </div>
  );
}
