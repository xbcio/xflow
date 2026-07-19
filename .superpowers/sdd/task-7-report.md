# Task 7 Report: M1 壳级 Playwright e2e 测试

## 状态

已完成。Playwright e2e 接入 `web/` workspace,覆盖 editor/viewer 健康页、路由、ErrorBoundary 与 viewer 只读,已接入 CI `web` job。`pnpm e2e`、`make web-ci`、`pnpm lint`、`pnpm typecheck` 全绿,YAML 可解析。

## Playwright 配置要点

- 配置文件:`web/playwright.config.ts`
- 测试目录:`web/e2e/`
- 浏览器:仅 chromium(M1 范围;firefox/webkit 留 M8)
- `webServer`:同时启动 editor(`pnpm --filter @xflow/app-workflow-editor dev`,端口 5173)与 viewer(`pnpm --filter @xflow/app-workflow-viewer dev`,端口 5174)
- `reuseExistingServer`:本地 true,CI(`process.env.CI`) false
- `retries`:CI 1 次,本地 0 次
- `reporter`:list + html,`outputDir: test-results`,`trace: on-first-retry`
- 无固定 baseURL,两个 app 分别用完整 `page.goto` 访问

## e2e 用例清单

### Editor(`web/e2e/editor.spec.ts`)

1. **健康页渲染**:访问 `/`,断言 app 名 `Workflow Editor`、版本 `0.1.0`、环境 `development`、mock `enabled`,以及 fixture 节点 `Start`/`End` 和连接 `start:default → end:default`。
2. **路由跳转**:访问 `/editor/some-workflow-id`,断言占位页面渲染 `Editor: some-workflow-id` 并展示 fixture JSON。
3. **ErrorBoundary 不泄漏**:访问 dev-only 的 `/__error`,断言显示 `Something went wrong`,且页面文本不含 `at `、`.tsx`、`.ts`、`Error:`、`ErrorTriggerPage` 等堆栈/路径/技术细节。

### Viewer(`web/e2e/viewer.spec.ts`)

1. **健康页渲染**:访问 `/`,断言 app 名 `Workflow Viewer`、版本、环境、mock 状态及 fixture 内容。
2. **路由跳转**:访问 `/view/some-workflow-id`,断言占位页面渲染 `Viewer: some-workflow-id` 并展示 fixture JSON。
3. **ErrorBoundary 不泄漏**:同 editor,访问 `/__error` 断言通用错误文案且无技术细节泄漏。
4. **Viewer 只读**:访问 `/view/some-workflow-id`,断言无 `button` 角色元素,且页面文本不匹配 `Save|Publish|Edit|Update|Delete|Create`。

总计 **7 条用例**,全部通过。

## ErrorBoundary 不泄漏的断言方式

ErrorBoundary fallback 只渲染:

```html
<div class="xflow-root error-boundary">
  <h1>Something went wrong</h1>
  <p>Please refresh the page or contact support if the problem persists.</p>
</div>
```

测试专用 `/__error` 路由仅在 `import.meta.env.DEV` 下注册,组件直接 `throw new Error("E2E intentional render error")`。e2e 断言:

- 正向:通用文案 `Something went wrong` 可见。
- 负向:页面 `body.innerText` 不含 `at `、`.tsx`、`.ts`、`Error:`、`ErrorTriggerPage`。

这样即使开发模式 React 会在 console 打印完整堆栈,UI 层仍不会泄漏敏感信息。

## CI 接入步骤

修改 `.github/workflows/ci.yml` 的 `web` job,在 `Build` 后新增:

1. `Install Playwright browsers`:`cd web && pnpm exec playwright install --with-deps chromium`
2. `Run e2e tests`:`cd web && pnpm e2e`
3. `Upload Playwright report`:失败时上传 `web/playwright-report/` 为 artifact

`Makefile` 的 `web-ci` 目标已追加 `web-e2e`:

```make
web-ci: web-install web-lint web-typecheck web-test web-check-boundaries web-build web-e2e
```

`web/package.json` 的 `e2e` script 已从占位改为 `playwright test`;`web-generate` 保持占位不变。

## 配置/脚本改动

- 新增 `web/playwright.config.ts`
- 新增 `web/e2e/editor.spec.ts`、`web/e2e/viewer.spec.ts`
- 新增 `web/apps/workflow-editor/src/pages/ErrorTriggerPage.tsx`
- 新增 `web/apps/workflow-viewer/src/pages/ErrorTriggerPage.tsx`
- 修改 `web/apps/workflow-editor/src/App.tsx`(dev-only `/__error` 路由)
- 修改 `web/apps/workflow-viewer/src/App.tsx`(dev-only `/__error` 路由)
- 修改 `web/package.json`(e2e script)
- 修改 `web/tsconfig.json`(包含 `e2e/**/*.ts` 与 `playwright.config.ts`)
- 修改 `web/tooling/eslint-config/index.js`(lint e2e 与 playwright config)
- 修改 `.github/workflows/ci.yml`
- 修改 `Makefile`

## 验证输出摘要

```bash
pnpm lint          # OK
pnpm typecheck     # OK
pnpm test          # OK (13 files / 15 tests)
pnpm check:boundaries # OK
pnpm build         # OK
pnpm e2e           # OK (7 passed)
make web-ci        # OK (含 e2e)
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" # OK
```

## Commit hash

- `8af2704` — `test(web): add Playwright e2e for Vite app shells`
- `4646b8b` — `ci(web): run e2e in web job`

## 遗留问题

- **prod preview e2e**:本期未对 production build 跑 `vite preview` e2e;M1.2 已通过 `grep dist/assets/index-*.js` 验证 `mockEnabled:!1`,后续可在 M8 扩展为真实 preview server e2e。
- **浏览器矩阵**:M1 仅 chromium;firefox/webkit 留 M8 兼容矩阵。
- **无真实 API**:e2e 仅验证 M1 壳,不覆盖 React Flow、Monaco、保存/发布等功能。
