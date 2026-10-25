import { defineConfig } from '@umijs/max';

import { devProxyAuthConfigured, proxy } from './proxy';
import { routes } from './routes';

/**
 * 网关的登录/鉴权入口，可为绝对 URL（https://sso.example.com/login）或同源
 * 相对路径（/gateway/login）。未设置时为空串：401 落到应用内的 /unauthorized，
 * 不再跳 config/routes.ts 里并不存在的 /login。
 */
const authLoginUrl = (process.env.XFLOW_AUTH_LOGIN_URL ?? '').trim();

/**
 * 开发代理会替浏览器认证：仅 `max dev`（NODE_ENV=development）且代理确实解析到
 * 令牌时为 true。注入前端的是这个布尔量，不是令牌——生产构建恒为 false，
 * 浏览器侧最多知道「有人替我做认证」，拿不到任何凭据。
 */
const devProxyAuth = process.env.NODE_ENV === 'development' && devProxyAuthConfigured;

export default defineConfig({
  // 刻意不启用两个插件（见设计文档 §2.3 / §2.4）：
  //   - antd 插件：会无条件注入裸 antd/dist/reset.css，压过全部 tailwind 工具类
  //   - layout 插件：依赖 @ant-design/pro-components（peer 仅 antd 5），会引入 antd 4 并存
  utoopack: {},
  tailwindcss: {},
  title: 'XFlow',

  request: {},
  model: {},
  initialState: {},
  access: {},
  // 站点只有中文：不跟随浏览器语言，避免 404 等走 intl 的少数文案变成英文。
  locale: { default: 'zh-CN', antd: true, baseNavigator: false },

  // esbuildMinifyIIFE 仅在回退到 webpack 时才需要（§2.8），utoopack 不走 esbuild 压缩
  npmClient: 'pnpm',
  monorepoRedirect: { srcDir: ['src'] },
  extraBabelIncludes: [/packages[/\\]xflow-/],

  routes,
  proxy,

  // 构建期替换 process.env.XFLOW_AUTH_LOGIN_URL 的字面量；值只来自构建环境，
  // 运行时无法注入。定义名与 src/auth.ts 的读取保持一致。
  // XFLOW_DEV_PROXY_AUTH 同理，只注入布尔量（见 src/devAuth.ts）。
  define: {
    'process.env.XFLOW_AUTH_LOGIN_URL': authLoginUrl,
    'process.env.XFLOW_DEV_PROXY_AUTH': devProxyAuth,
  },
});
