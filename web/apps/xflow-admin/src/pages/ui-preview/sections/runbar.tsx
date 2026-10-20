import { runVariants } from '../data';
import { RunBar } from '../components/RunBar';
import { workflowStatusMeta } from '../meta';

/**
 * Run bar wall: one bar per `ExecutionStatus`, all nine of them, with the
 * operations generated from `workflowStatusMeta` — the section exists to prove
 * the bar has no status-specific button logic of its own, and to show the
 * terminal / queued / canceling shapes the running workbench never renders.
 */
export function RunBarSection() {
  return (
    <div className="xflow-pv-runwall">
      {runVariants.map((variant) => {
        const status = workflowStatusMeta[variant.status];
        return (
          <div className="xflow-pv-runwall-row" key={variant.status} data-status={variant.status}>
            <span className="xflow-pv-runwall-key">
              <span className="xflow-pv-mono">{variant.status}</span>
              <span className="xflow-pv-runwall-label">{status.label}</span>
            </span>
            <div className="xflow-pv-runwall-strip">
              <div className="xflow-pv-float-row">
                <RunBar variant={variant} />
              </div>
            </div>
            <span className="xflow-pv-runwall-ops">
              可用操作：{status.ops.length > 0 ? status.ops.join(' · ') : '无（等在途任务提交）'}
            </span>
          </div>
        );
      })}
    </div>
  );
}
