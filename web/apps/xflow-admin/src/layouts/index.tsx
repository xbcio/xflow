import { App, ConfigProvider, Layout, theme } from 'antd';
import { Outlet } from '@umijs/max';

/**
 * 应用布局。
 *
 * 这里承担三件插件无法代劳的事（不启用 layout 插件，原因见设计文档 §2.3）：
 *   1. ConfigProvider —— 主题 token。刻意不设 locale：locale 插件已在更外层注入了
 *      一层带 locale + direction 的 ConfigProvider，此处只管主题，避免覆盖语言切换。
 *   2. App —— antd 6 静态方法（message / Modal.confirm）的上下文宿主。
 *   3. Layout + Outlet —— 内容区骨架。
 *
 * 导航菜单为最小骨架，待业务页面成型后再细化。
 *
 * 注意 minHeight 用了 style 而非 tailwind 的 min-h-screen：antd 组件样式由 cssinjs
 * 在运行时注入，规则不在任何 CSS layer 内，因此会压过 @layer utilities 里的 tailwind
 * 工具类（.ant-layout 自带 min-height:0）。tailwind.css 的 layer 方案只解决静态
 * reset.css，管不到 cssinjs。规律：不要用 tailwind 工具类去覆盖 antd 组件已声明的属性，
 * 改用 style / antd token；tailwind 用于 antd 未声明的属性和自有元素则完全正常。
 */
export default function BasicLayout() {
  return (
    <ConfigProvider theme={{ algorithm: theme.defaultAlgorithm }}>
      <App>
        <Layout style={{ minHeight: '100vh' }}>
          <Layout.Header className="flex items-center">
            <span className="text-white text-base font-semibold">XFlow</span>
          </Layout.Header>
          <Layout.Content className="p-6">
            <Outlet />
          </Layout.Content>
        </Layout>
      </App>
    </ConfigProvider>
  );
}
