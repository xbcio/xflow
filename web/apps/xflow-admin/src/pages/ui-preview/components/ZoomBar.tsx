import {
  ApartmentOutlined,
  ColumnWidthOutlined,
  ExpandOutlined,
  MinusOutlined,
  PictureOutlined,
  PlusOutlined,
} from '@ant-design/icons';
import { Slider } from 'antd';

import { Float, FloatButton, VerticalSep } from './primitives';

/**
 * Bottom-left canvas tool bar (§6.2): zoom − / slider (10%–200%) / value / +,
 * then fit, auto layout, and the ruler / minimap toggles. Fit re-applies the
 * fitted zoom after a manual zoom, exactly like fitView in the real editor.
 */
export function ZoomBar({
  scale,
  onScaleChange,
  onFit,
  ruler,
  onRulerChange,
  minimap,
  onMinimapChange,
}: {
  scale: number;
  onScaleChange: (scale: number) => void;
  onFit: () => void;
  ruler: boolean;
  onRulerChange: (visible: boolean) => void;
  minimap: boolean;
  onMinimapChange: (visible: boolean) => void;
}) {
  const percent = Math.round(scale * 100);

  return (
    <Float className="xflow-pv-zoombar">
      <FloatButton
        label="缩小"
        icon={<MinusOutlined />}
        onClick={() => onScaleChange(Math.max(0.1, (percent - 10) / 100))}
      />
      <Slider
        className="xflow-pv-zoom-slider"
        min={10}
        max={200}
        value={percent}
        onChange={(value) => onScaleChange(value / 100)}
        tooltip={{ formatter: (value) => `${value}%` }}
      />
      <span className="xflow-pv-zoom-value">{percent}%</span>
      <FloatButton
        label="放大"
        icon={<PlusOutlined />}
        onClick={() => onScaleChange(Math.min(2, (percent + 10) / 100))}
      />
      <VerticalSep />
      <FloatButton label="适应画布" icon={<ExpandOutlined />} onClick={onFit} />
      <FloatButton label="自动布局" icon={<ApartmentOutlined />} />
      <FloatButton
        label="标尺 (⇧R)"
        icon={<ColumnWidthOutlined />}
        active={ruler}
        onClick={() => onRulerChange(!ruler)}
      />
      <FloatButton
        label="缩略图 (⇧M)"
        icon={<PictureOutlined />}
        active={minimap}
        onClick={() => onMinimapChange(!minimap)}
      />
    </Float>
  );
}
