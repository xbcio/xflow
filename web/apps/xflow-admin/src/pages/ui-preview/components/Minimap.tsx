import { RULER } from '../metrics';
import type { NodeRect } from './useCanvasMetrics';

const WIDTH = 200;
const HEIGHT = 100;

/**
 * Minimap (§6.2): 200×100, off by default, toggled from the zoom bar. Node rects
 * come from the measured canvas DOM, so the blocks track what the cards render.
 * The height is what keeps the panel off the false-branch stack below it — see
 * `.xflow-pv-minimap` for why it is shorter than the mockup's box.
 */
export function Minimap({
  rects,
  canvasWidth,
  canvasHeight,
  scale,
}: {
  rects: NodeRect[];
  canvasWidth: number;
  canvasHeight: number;
  scale: number;
}) {
  const viewWidth = Math.max(1, (canvasWidth - RULER) / scale);
  const viewHeight = Math.max(1, (canvasHeight - RULER) / scale);
  const maxX = Math.max(viewWidth, ...rects.map((rect) => rect.x + rect.w)) + 40;
  const maxY = Math.max(viewHeight, ...rects.map((rect) => rect.y + rect.h)) + 40;
  const factor = Math.min(WIDTH / maxX, HEIGHT / maxY);
  const offsetX = (WIDTH - maxX * factor) / 2;
  const offsetY = (HEIGHT - maxY * factor) / 2;

  return (
    <div className="xflow-pv-minimap" role="img" aria-label="缩略图">
      {rects.map((rect) => (
        <span
          key={rect.id}
          className="xflow-pv-mm-node"
          data-tone={rect.tone}
          style={{
            left: offsetX + rect.x * factor,
            top: offsetY + rect.y * factor,
            width: rect.w * factor,
            height: Math.max(3, rect.h * factor),
          }}
        />
      ))}
      <span
        className="xflow-pv-mm-view"
        style={{
          left: offsetX,
          top: offsetY,
          width: viewWidth * factor,
          height: viewHeight * factor,
        }}
      />
    </div>
  );
}
