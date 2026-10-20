import type { ReactNode } from 'react';

import { adminCluster, adminNavGroups } from '../data';
import { typeIcon } from '../meta';

/**
 * 管理端外壳（§6.6）：左导航 + 主区。工作流列表和运行记录用的是同一个壳 ——
 * 两节只有顶部工具条和主体不同，导航、命名空间和集群状态是同一份数据，
 * 所以它们不能各写一遍，否则「当前页」和计数迟早对不上。
 *
 * `current` 由调用方给：数据层不知道这一屏渲染的是哪一节，导航高亮属于视图状态。
 *
 * 集群状态（Runner / Leader）来自 `/v1/management/*`，服务端没开管理模块时整组
 * 不存在 —— 这里照静态稿画出来，但它是不是存在由宿主决定，不是导航自己判断。
 */
export function AdminShell({
  current,
  top,
  children,
}: {
  current: string;
  /** 顶部工具条：两节的搜索框、筛选与主操作都不一样。 */
  top: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="xflow-pv-admin">
      <nav className="xflow-pv-admin-nav" aria-label="主导航">
        <div className="xflow-pv-admin-brand">
          <i aria-hidden="true" />
          XFlow
        </div>
        {/* 命名空间选择器：静态预览里是展示态，接上后台后才是控件（§6.6）。 */}
        <div className="xflow-pv-admin-ns">
          <span>{adminCluster.pod}</span>
        </div>
        {adminNavGroups.map((group) => (
          <div key={group.id}>
            <div className="xflow-pv-admin-group">{group.label}</div>
            {group.items.map((item) => (
              <div
                key={item.id}
                className="xflow-pv-admin-item"
                data-current={item.id === current ? '' : undefined}
                aria-current={item.id === current ? 'page' : undefined}
              >
                <span className="xflow-pv-admin-item-icon">{typeIcon[item.icon]}</span>
                {item.label}
                {item.count === undefined ? null : (
                  <span className="xflow-pv-admin-item-count">{item.count}</span>
                )}
              </div>
            ))}
          </div>
        ))}
        <div className="xflow-pv-admin-cluster" aria-label="集群状态">
          <div>
            <span>Runner</span>
            <span data-tone="ok">{adminCluster.runners}</span>
          </div>
          <div>
            <span>Leader</span>
            <span>{adminCluster.leader}</span>
          </div>
        </div>
      </nav>

      <div className="xflow-pv-admin-main">
        {/* 工具条内容整条由调用方给：搜索框、筛选、主操作两节都不一样。 */}
        <div className="xflow-pv-admin-top">{top}</div>
        <div className="xflow-pv-admin-body">{children}</div>
      </div>
    </div>
  );
}
