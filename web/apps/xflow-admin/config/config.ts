import { defineConfig } from '@umijs/max';

import { proxy } from './proxy';
import { routes } from './routes';

export default defineConfig({
  // 刻意不启用两个插件（见设计文档 §2.3 / §2.4）：
  //   - antd 插件：会无条件注入裸 antd/dist/reset.css，压过全部 tailwind 工具类
  //   - layout 插件：依赖 @ant-design/pro-components（peer 仅 antd 5），会引入 antd 4 并存
  utoopack: {},
  tailwindcss: {},

  request: {},
  model: {},
  initialState: {},
  access: {},
  locale: { default: 'zh-CN', antd: true, baseNavigator: true },

  // esbuildMinifyIIFE 仅在回退到 webpack 时才需要（§2.8），utoopack 不走 esbuild 压缩
  npmClient: 'pnpm',
  monorepoRedirect: { srcDir: ['src'] },
  extraBabelIncludes: [/packages[/\\]xflow-/],

  routes,
  proxy,
});
