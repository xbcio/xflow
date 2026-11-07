import { NodeLibrary, Outline } from '../components/LeftPanels';
import { wbLibraryCommon, wbLibraryGroups, wbLibraryTotal, wbOutline, wbOutlineTotal } from '../data';

/**
 * 左栏样例（§6.0.1 左栏重设计）：工作台那一份是可交互的运行视图，这里补齐
 * 其余状态——触发器组展开 / 折叠、搜索命中 / 无命中、大纲编辑视图（无状态点）
 * 与运行视图（有状态点）。全部使用同一套组件与 data.ts 数据。
 */
export function LeftSamplesSection() {
  return (
    <div className="xflow-pv-gallery xflow-pv-left-samples">
      <figure className="xflow-pv-figure">
        <div className="xflow-pv-left-sample">
          <NodeLibrary
            groups={wbLibraryGroups}
            common={wbLibraryCommon}
            total={wbLibraryTotal}
            expanded={['common', 'trigger']}
          />
        </div>
        <figcaption>
          节点库：常用组在最上（默认集补齐 6 个），触发器组展开；其余组折叠，组头高 28、可点、带 aria-expanded
        </figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <div className="xflow-pv-left-sample">
          <NodeLibrary
            groups={wbLibraryGroups}
            common={wbLibraryCommon}
            total={wbLibraryTotal}
            initialSearch="http"
          />
        </div>
        <figcaption>搜索命中：输入即展开所有组并只显示命中项，命中字高亮</figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <div className="xflow-pv-left-sample">
          <NodeLibrary
            groups={wbLibraryGroups}
            common={wbLibraryCommon}
            total={wbLibraryTotal}
            initialSearch="zzz"
          />
        </div>
        <figcaption>搜索无命中：写「没有匹配的节点」，下面给「清除搜索」</figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <div className="xflow-pv-left-sample">
          <Outline rows={wbOutline} total={wbOutlineTotal} mode="edit" selectedId="fetch_profile" disabledIds={['notify_sales']} />
        </div>
        <figcaption>
          大纲 · 编辑视图：不画状态点，被禁用的节点画 BanIcon；当前选中项 primary 浅底 + 左侧 2px 条
        </figcaption>
      </figure>

      <figure className="xflow-pv-figure">
        <div className="xflow-pv-left-sample">
          <Outline rows={wbOutline} total={wbOutlineTotal} mode="run" selectedId="fetch_profile" />
        </div>
        <figcaption>大纲 · 运行视图：状态点按状态表取色，条目高 28、20px 分类图标</figcaption>
      </figure>
    </div>
  );
}
