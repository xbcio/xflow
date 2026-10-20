import { NodeCard } from '../components/NodeCard';
import { Todo } from '../components/Todo';
import { cardGallery } from '../data';

/**
 * 2b · every node kind the card summary has to cover: long values truncate,
 * missing required values go red, credentials show only the reference name and
 * secrets are masked. The summaries are the real `NodeCard` cards, so a change
 * in the card shows up here.
 *
 * The whole gallery is marked `需新字段`: which rows a kind declares comes from a
 * `card_summary` descriptor field that does not exist on the backend yet.
 */
export function CardPropsSection() {
  return (
    <Todo label="需新字段" className="xflow-pv-todo-wrap--block">
      <div className="xflow-pv-card-gallery">
        {/* 画廊里的一张卡 = 一种节点类型的样例；id 挂在图上，断言按它取某张卡的
            摘要行文字（H1 / S13 的「审批超时」就长在这张卡上）。 */}
        {cardGallery.map((sample) => (
          <figure key={sample.id} data-card-gallery={sample.id}>
            <NodeCard node={sample.node} variant="static" />
            <figcaption>{sample.caption}</figcaption>
          </figure>
        ))}
      </div>
    </Todo>
  );
}
