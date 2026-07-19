# M0 ADR 草案：前端工作流管理工程技术决策

> **状态**：已采纳(2026-07-19);F0 followup 更新 pin_data 决策为计入 semantic hash(见 D4)  
> **范围**：M0 前端工程(Editor / Viewer / Admin)  
> **依据**：Task 2 brief、`AGENTS.md`、`docs/design/`、`types/`、`service/apiserver/` 现状、M0.2 definitionHash 核查报告(task-1-report.md)。  
> **包结构对齐**：本 ADR 的包命名与计划 `.claude/plans/2026-07-19-frontend-workflow-management-system.md` 第 1.2 节一致 —— 9 个公共 package(`workflow-core`、`workflow-renderer`、`workflow-provider`、`workflow-editor`、`workflow-viewer`、`workflow-components`、`node-registry`、`api-client`、`embed-sdk`),3 个 app(`workflow-editor`、`workflow-viewer`、`admin`)。npm scope 统一为 `@xflow/*`。D7/D8 中早期草稿出现的 `@xflow/types`/`editor-core`/`viewer-core`/`ui` 等简化名已作废,以本节及 D7、D8 为准。
>
> **⚠️ Scope 变更(2026-07-19 用户决策)**:**Admin 应用本期不实现,推迟到后续迭代**。据此:
> - M1.3(官方 Umi 脚手架生成 `apps/admin`)取消;`apps/admin` 目录本期不创建。
> - M6(Umi Admin 整个阶段)整体推迟。
> - ADR 中 Admin 专属决策(D6 IAM 边界、D1 中 `@umijs/max`/`antd`/`@ant-design/pro-components` 版本锁定、D2 中 Admin 部署、D8 依赖图中的 `apps/admin` 节点)**保留为未来 Admin 实施时的预决策,本期不落地、不阻塞 M1**。
> - D8"公共包禁止依赖 Umi/ProLayout/Admin alias"规则**仍然保留**——这是公共包的边界约束,与 Admin 是否实现无关,本期公共包即按此约束构建。
> - 本期 M1 实际执行:M1.1(workspace + tooling,不含 `apps/admin`)、M1.2(两个 Vite App:editor + viewer)、M1.4(Makefile + CI 前端门禁,不含 Umi Admin 构建)。

---

## 摘要

本 ADR 锁定 M0 前端工程的 8 项技术决策：

1. 技术栈与版本号全部写死（Node 22 LTS、pnpm 10、React 19、Umi 4、Ant Design 5 等）。
2. Editor / Viewer App 在同一域名下按路径独立部署，组件包始终独立发布。
3. 后端 DSL 的 canonical wire format 为 JSON；YAML 仅作为导入/导出投影，不保留注释/锚点/顺序。
4. `WorkflowDef` 中影响执行与 hash 的字段与编辑器元数据（Position / UI / Notes / Viewport）严格分离；待 M0.2 确认当前 hash 是否已剥离 editor 字段。
5. API 鉴权复用后端 `BearerTokenAuth` / `PrincipalAuth` 机制；CORS 采用显式 allowlist；iframe embed 使用短期 HMAC JWT（TTL 5 min），禁止长期 token 入 URL。
6. Admin IAM 不接本地用户/角色 CRUD，默认占位接入企业 OIDC/SAML；前端 `src/access.ts` 定义 read/edit/publish/signal/cancel/deadletter 六类权限，后端仍作最终鉴权。
7. 浏览器矩阵为 Evergreen 最新两个主版本；npm 包统一 `@xflow/*` scope，公共 API 稳定前全部 private / pre-release。
8. 公共组件包禁止依赖 `@umijs/max`、Admin 的 `@/` alias、`@ant-design/pro-layout`；通过 `eslint-plugin-import` + CI boundary check 强制执行。
9. (D9) 状态管理与服务端数据:server state 用 ahooks `useRequest`(配合生成的 server 调用代码,无全局实例,适配生成器)替代 TanStack Query;画布图模型 + undo/redo 命令栈用 zustand(selector 订阅/不可变/中间件,满足 100/500 节点性能门禁);零散交互态用 ahooks 其余 hooks;两者仅 app 层,公共包禁。

---

## D1. 技术栈版本锁定

### Context

M0 需要交付可维护、可复现的前端工程基线。版本号必须写死，不能写“最新稳定版”，且需区分 peerDependency / devDependency / runtime dependency。

### Decision

| 依赖 | 锁定版本 | 类型 | 说明 |
|------|----------|------|------|
| Node.js | `22.15.0` LTS | 运行时 | Active LTS，支持到 2027-04；与 pnpm 10 兼容性已验证。 |
| pnpm | `10.10.0` | 包管理器 | 通过 `packageManager` 字段锁定；workspace + changesets 工作流成熟。 |
| React | `19.1.0` | peerDependency | 公共组件包设为 peer；App 设为 runtime dependency，避免多实例。 |
| React DOM | `19.1.0` | peerDependency / runtime | 同 React。 |
| TypeScript | `5.8.3` | devDependency | 支持 `erasableSyntaxOnly` 等最新编译选项； monorepo 统一 tsc。 |
| `@umijs/max` | `4.4.8` | runtime（仅 Admin App） | Umi 4 为当前主版本；仅 Admin App 可依赖（Viewer/Editor 为 Vite 应用，不依赖 Umi），公共包禁止。 |
| `antd` | `5.26.0` | runtime | Ant Design 5 稳定，与 Umi 4 / React 19 兼容。 |
| `@ant-design/pro-components` | `2.8.8` | runtime（仅 Admin App） | ProTable / ProForm 降低 Admin CRUD 成本；不进入公共组件包。 |
| `tailwindcss` | `3.4.17` | devDependency（组件包）/ runtime（App） | v3 与 Umi 4 集成稳定；M0 不追 v4，避免构建插件改造。 |
| `@xyflow/react` | `12.5.0` | runtime（Editor/Viewer） | React Flow v12 为当前主版本，支持 React 19。 |
| `@monaco-editor/react` | `4.7.0` | runtime（Editor） | Monaco 加载器，与 React 19 兼容。 |
| `ahooks` | `3.8.4` | runtime（仅 app 层） | **必引**：server state 用 `useRequest`(配合生成的 server 调用代码,见 D9) + editor/viewer 交互态/副作用(debounce/effect/localStorage 等)；React 19 兼容；公共包禁止。 |
| `zustand` | `5.0.5` | runtime（仅 app 层） | Editor 画布图模型 + undo/redo 命令栈(性能门禁与命令栈中间件,见 D9)；公共包禁止。 |
| ~~`@tanstack/react-query`~~ | — | **不引入** | server state 改用 ahooks `useRequest` + 生成的 server 调用代码(见 D9 职责切分与理由)。 |
| `vitest` | `3.2.0` | devDependency | 单元/组件测试；与 Vite 生态一致。 |
| `playwright` | `1.52.0` | devDependency | E2E；测试 Viewer 渲染与 Admin 关键路径。 |
| ESLint | `9.25.0` | devDependency | flat config；统一 monorepo lint 规则。 |
| `@changesets/cli` | `2.29.0` | devDependency | 版本管理与 changelog；M0 pre-release 阶段即启用。 |

**peerDependency 规则**：

- `@xflow/types`、`@xflow/editor-core`、`@xflow/viewer-core` 必须将 `react`、`react-dom` 声明为 `peerDependencies`，范围写死 `"19.1.0"`（M0 不放宽）。
- Admin / Viewer App 作为最终消费者，将 `react`、`react-dom` 声明为普通 `dependencies`。
- 公共组件包禁止把 `@umijs/max`、`antd`、`@xyflow/react` 等 App 级依赖纳入自身 `dependencies`；需要的通过 peer 或 optional peer 声明。

### 备选方案

- **Node 20 LTS**：已被 22 LTS 替代，生命周期更短，不推荐。
- **pnpm 9.x**：功能差异小，但 pnpm 10 为当前推荐版本，且默认启用更严格的 `onlyBuiltDependencies` 行为。
- **React 18.3.1**：生命周期末期，Umi 4 虽兼容，但 React 19 已稳定半年以上，且 React Flow v12 对 19 支持更好。
- **Umi 5 / Ant Design 6**：尚未发布或未达到 M0 稳定要求，不纳入。

### 风险

- React 19 与部分 Umi 4 插件可能存在边界兼容问题，需在项目初始化后跑通 `pnpm dev/build/test` 作为 gate。
- `@umijs/max` 4.4.8 若后续 patch 有 break，需通过 pnpm 补丁或锁死版本应对；M0 不允许自动升级 minor。
- peerDependency 配置错误会导致 Editor/Viewer 嵌入宿主应用时出现多 React 实例；CI 需运行 `pnpm why react` 检测重复。

---

## D2. Editor / Viewer App 发布边界

### Context

M0 需要决定 Editor App 与 Viewer App 是独立域名、同域不同路径，还是仅作为组件包。必须考虑 iframe 嵌入、CORS、独立部署节奏，同时组件包必须独立。

### Decision

**推荐：组件包独立；Editor App 与 Viewer App 同一域名、不同路径独立部署。**

- **组件包**:`@xflow/workflow-editor`、`@xflow/workflow-viewer` 必须独立 npm 包,可被第三方宿主应用直接嵌入。
- **Editor App**:`/editor/*` 路径,面向工作流设计师;Vite 构建。
- **Viewer App**:`/view/*` 路径,面向只读展示/审批;Vite 构建。
- 同一域名部署,共享顶级域名 cookie / SSO session,减少 CORS 和 token 传递复杂度。
- 两个 App 拥有独立 `dist` 产物和独立 CI pipeline,可分别灰度发布。
- Admin App 单独使用锁定版本的官方 Umi/Ant Design Pro 脚手架(见 D1),与两个 Vite App 共用 `@xflow/*` 公共包。

### 备选方案

- **独立子域（editor.xflow.example、view.xflow.example）**：部署节奏更灵活，但需要跨域 SSO、CORS allowlist、iframe token 传递更复杂；仅在明确需要独立域名时采用。
- **合并为一个 App，通过路由切换编辑/只读模式**：降低部署成本，但违背“Viewer 应轻量、Editor 重”的边界；不推荐。

### 风险

- 同域路径方案下，若未来需要把 Viewer 开放给外网、Editor 仅限内网，会需要拆分；前期保留独立构建产物，拆分成本低。
- iframe 嵌入场景即使同域也可能需要 token；见 D5。

---

## D3. JSON / YAML 策略与版本概念

### Context

DSL 在后端以 Go `types.WorkflowDef` 为权威，前端需要明确 wire format、YAML 角色以及多种 version 的边界。

### Decision

**JSON 为 canonical wire format；YAML 为导入/导出投影。**

- 所有与后端 `service/apiserver` 的交互（`POST /v1/workflows`、`POST /v1/workflows/invoke` 等）使用 JSON。
- 前端 DSL 内存模型与 `types.WorkflowDef` JSON 字段一一对应，不引入额外字段。
- YAML 仅在“文件导入/导出”功能中出现：
  - 导入：YAML → JSON → `WorkflowDef`。
  - 导出：`WorkflowDef` → JSON → YAML。
  - 不保留 YAML 注释、锚点（`&` / `*`）、key 顺序；导出后的 YAML 是规范投影，不是原文本 round-trip。
  - 导入失败时向用户提示具体行号/字段，但后端校验以 JSON 语义为准。

**版本概念矩阵**：

| 版本名 | 所在位置 | 生成方式 | 语义 |
|--------|----------|----------|------|
| DSL spec version | `WorkflowDef.spec` | 手写 / UI 默认填 `"1.0"` | 语法与编译器契约版本；后端 `graph.Compile` 据此做兼容性校验。 |
| Workflow ID | `WorkflowDef.id` | UI 创建时生成 UUID v4；导入时保留或重新生成 | 工作流全局唯一标识，不变更则视为同一工作流。 |
| Draft revision | 编辑器 draft 状态 / 后端 draft 表 | 每次保存自增；M0 可先用前端本地版本号 + 时间戳 | 未发布草稿的编辑历史；不影响运行时。 |
| 发布业务 version | `WorkflowDef.version` | 用户手动升版，遵循 SemVer `1.0.0` | 对外发布的工作流版本；后端按 workflow ID + version 注册。 |
| Handler version | `NodeDef.version` | 用户为节点选择 handler 主版本；省略则使用最新 | 节点类型实现版本；dispatch 时精确匹配。 |
| Catalog version | 前端节点目录 / `@xflow/types` | 与 npm package version 同步或独立自增 | 节点类型、Descriptor、ParamSpec 的 schema 版本。 |
| API version | URL path | 固定 `/v1/*` | HTTP API 大版本；M0 不暴露 `/v2`。 |
| npm package version | `package.json` | Changesets 管理 | `@xflow/*` 包版本；公共 API 稳定前保持 `0.x` 或 prerelease。 |

### 备选方案

- **YAML 作为 canonical wire format**：与后端 `types.WorkflowDef` JSON tag 不一致，需要额外转换层，且 YAML 解析器差异大；拒绝。
- **保留 YAML 注释 round-trip**：需要在前端维护 AST + 注释映射，M0 成本过高；作为后续增强项记录。

### 风险

- 用户可能期望“导出再导入”完全还原原文本，需 UI 明确提示“YAML 为规范投影，不保留注释和格式”。
- `WorkflowDef.spec` 与后端编译器版本不一致时会导致 `400`；前端需在保存/发布前校验 spec 字段。

---

## D4. `WorkflowDefinition` / `WorkflowEditorMetadata` 边界

### Context

`types.WorkflowDef` 与 `types.NodeDef` 中既包含运行时数据，也包含编辑器元数据。必须明确哪些字段影响 hash 与执行，哪些仅用于 UI。M0.2 正在核查 `definitionHash` 是否含 `Position` / `UI`。

### Decision

**运行时定义字段(影响执行与 semantic hash)**:

- `WorkflowDef`:`namespace`、`name`、`version`、`description`、`spec`、`runnerSelector`、`context`、`settings`、`options`、`credentials`、`params`、`node_templates`、`nodes`(运行时相关子字段)、`connections`、`outputs`、`pin_data`。
- `NodeDef`:`id`、`name`、`type`、`kind`、`version`、`template`、`disabled`、`on_error`、`runnerSelector`、`inputs`、`output_schema`、`parameters`、`retry`。

**编辑器元数据字段(不影响执行与 semantic hash)**:

- `WorkflowDef`:无顶层 UI 字段(当前 `types` 中未定义),未来如需 `viewport`、`canvasLayout` 等放入独立的 `WorkflowEditorMetadata`。
- `NodeDef`:`position`、`notes`、`ui`。

**明确排除的实例标识字段**:

- `WorkflowDef.ID`:仅标识工作流实例/记录,不是运行时语义;重生成 ID 不应改变 runtime hash。
- `WorkflowDef.TenantID`:服务端从认证上下文注入,`json:"-"` 已不参与 marshal,同样不参与 runtime hash。

**`pin_data` 计入 semantic hash**(2026-07-19 followup 用户决策):pin_data 固定节点输入数据,改变会导致执行输出不同,属执行语义而非展示元数据,与 `position`/`ui`/`notes`/`viewport` 排除规则不冲突。

**M0.2 核查结论(已确认)**:Task 1 核查报告证实 —— 后端 `sdk/xflow/workflow_identity.go:16` 的 `definitionHash` 直接 `json.Marshal(def)`,`NodeDef.Position`(带 `json:"position,omitempty"` tag)和 `NodeDef.UI`(`json:"ui,omitempty"`)都会进入 hash;实测修改 `Position`/`UI` 会改变 `definitionHash`。运行时编译器(`engine/graph/compile.go` 的 `registerNodes`)不读取这两个字段,`engine/graph/snapshot.go:211-233` 的 `assignGraphHash` 已经排除 editor metadata。因此现状是:**`definitionHash`(用于注册表 key 级冲突检测)与 `graph hash`(运行时图身份)语义不一致** —— 编辑器接入后,用户仅拖动画布就会改变 `definitionHash`,触发 `backend.ErrWorkflowConflict`(`backend/local/workflow_registry.go:32`、`backend/distributed/internal/workflowreg/registry.go:93-95` 均按 `DefinitionHash` 判冲突)。

**F0.3 实施后状态(后端)**:

1. 新增 `sdk/xflow/runtimeHash(def)` helper,使用结构体字段顺序固定的 normalized payload 做 `json.Marshal` + SHA-256,格式为 `runtime-sha256:v1:<hex>`。
2. `Engine.AddWorkflow`(`sdk/xflow/workflow_registry.go`)已改用 `runtimeHash` 生成 `WorkflowRecord.DefinitionHash`;本地/分布式注册表继续用该字段做 key 级冲突检测,比较的是运行时语义。
3. 原 `definitionHash(def)` 重命名为 `legacyDefinitionHash(def)`,格式为 `sha256:audit:v1:<hex>`,作为 `WorkflowRecord.AuditFingerprint` 持久化,用于审计/导出追溯。
4. Editor metadata(`position`/`ui`/`notes`)不再影响注册表冲突检测;`pin_data` 计入 runtime hash。
5. 兼容策略采用**选项 B**:version bump 时自然采用新 hash。本期 prototype 分支无存量生产数据,不遍历重算存量 `definition_hash`;存量记录视为 legacy hash,运行时语义未变即可继续读取。

**边界规则**:

1. 前端保存草稿时,完整保留 `WorkflowDef` + `WorkflowEditorMetadata`。
2. 前端"发布"或调用 `POST /v1/workflows` 时,必须向后台提交**运行时定义**;`position`、`ui`、`notes`、`viewport` 不应进入 canonical execution hash;`pin_data` **计入** semantic hash(见上)。
3. Editor metadata 必须按 `NodeDef.ID` 索引(而不是 `NodeDef.Name`);`NodeDef.Name` 仅用于 DSL connection 和表达式引用。Go 侧当前无 editor metadata 持久化路径,未来 M2 Repository 实现时按 node ID 约束。
4. 既有 `WorkflowDef` 可继续读取(`Position`/`UI` 字段保留在 `types.NodeDef` 中,反序列化不受影响);已发布版本运行时语义不变。
5. 推荐在 M1/M3 即引入前端 `WorkflowEditorMetadata` 类型(`@xflow/workflow-core` 中,与 `WorkflowDef` 并列),将 `positions`、`viewport`、`ui`、`notes`、`pinData` 移入其中,彻底避免污染运行时定义;提交发布时剥离。
6. **草稿 revision 乐观锁**独立于 hash —— 仓库当前无 draft/revision 概念(grep `draft`/`revision` 无结果),draft 的并发控制使用独立的 `revision` 整数/version vector,不依赖 `definitionHash`。

### 备选方案

- **所有字段进入 hash**:实现最简单,但会导致"挪动节点位置"触发新版本,违背业务预期;拒绝。
- **运行时定义与编辑器元数据完全分两个 API 保存**:更清晰,但 M0 改动较大;可作为 M1 演进目标,M0 先保证发布时剥离。

### 风险

- `definitionHash` → `runtimeHash` 的迁移会改变 `DefinitionHash` 字段语义(从全字段指纹变为 runtime hash),需同步更新字段注释、文档,以及 `backend/local`、`backend/distributed` 注册表的冲突检测测试。该迁移属后端工作,不在本次 M0(决策+基线)范围,记入 M2 backlog。
- `Description`/`Nodes[].Notes` 是否一并排除 editor metadata 需在 M2 后端迁移时定夺;M0 ADR 暂定排除以与 `assignGraphHash` 对齐。
- 用户可能把 `notes` 误以为是运行时注释,UI 需明确标注"仅编辑器可见"。

---

## D5. API 鉴权 / CORS / iframe token / allowed origin

### Context

`service/apiserver` 现状：

- 已提供 `BearerTokenAuth`（静态 bearer）和 `BearerPrincipalAuth`（bearer + subject/scopes）。
- 已定义 `PrincipalAuth` + `Authorizer` + `AuditSink` 的 B3 路径。
- 当前无 CORS 中间件；无 iframe embed token 机制。
- 组织安全策略要求：token 必须有 timeout、禁止长期 token 入 URL、敏感数据不进 URL 参数。

### Decision

**API 鉴权**：

- Admin / Editor / Viewer 前端统一使用 `Authorization: Bearer <token>` 头部。
- token 来源：
  - 企业 OIDC IdP 颁发的 access token（M0 占位方案，实际对接由部署侧完成）。
  - 或由 xflow 后端提供短期 session token（例如 15 min access + 7d refresh，refresh 用 HttpOnly Secure SameSite=Strict cookie）。
- 后端通过 `PrincipalAuthenticator` 校验 token，映射为 `Principal{Subject, TenantID, Scopes}`，再经 `ScopeAuthorizer` 做操作级鉴权。
- M0 实现顺序：先用 `BearerTokenAuth` / `BearerPrincipalAuth` 跑通；M0.5 接入真实 OIDC validator 替换静态 bearer。

**CORS 策略**：

- 后端新增 CORS 中间件，配置 `ALLOWED_ORIGINS` 列表（显式 allowlist），默认 deny。
- 同域部署时 `ALLOWED_ORIGINS` 可只填自身域名；iframe 嵌入场景按 D2 独立域名或跨域需求追加。
- 允许的方法：`GET, POST, PUT, DELETE, OPTIONS`；允许的头部：`Authorization, Content-Type, X-Request-ID`。
- 不允许通配符 `*`，不允许反射 `Origin`。

**iframe embed token**：

- 推荐方案：短期 HMAC-SHA256 JWT，TTL 5 分钟，签发端为 xflow 后端 `/v1/auth/iframe-token`。
- JWT claims：`sub`（工作流/用户标识）、`aud`（允许嵌入的 origin）、`exp`、一次性 `jti`。
- 签发前校验调用方已登录且对目标资源有 `read` 权限。
- iframe 加载时通过 `postMessage` 或 HTML `data-token` 属性将 token 传入前端 Viewer，Viewer 再用 `Authorization: Bearer <iframe-token>` 调用 API。
- **禁止将长期 access token / refresh token 放入 URL query 或 fragment**；符合组织安全策略。
- 可选增强：jti 在 Redis 中标记 5 min 内只能使用一次，防止 replay。

**`frame-ancestors` / CSP**：

- 后端响应增加 CSP header：
  - `Content-Security-Policy: frame-ancestors 'self' https://allowed-origin.example.com;`（仅允许同域和 allowlist 中的 origin 嵌入）。
  - 或返回 `X-Frame-Options: SAMEORIGIN`；当需要跨域 iframe 时改为 `frame-ancestors` 显式列表。
- API 响应不渲染 HTML，主要防止 Viewer App 被任意站点嵌入钓鱼。

### 备选方案

- **长期 iframe URL 签名（如 `?token=xxx`）**：违反组织安全策略“长期 token 禁止进入 URL”，拒绝。
- **依赖浏览器 `document.domain` 跨域**：已弃用且不安全，拒绝。
- **CORS 通配符 origin**：违反安全策略，拒绝。

### 风险

- OIDC 占位方案到真实对接之间存在 gap；M0 必须明确“身份源对接由部署侧提供 validator”的边界。
- 短期 iframe token 的时钟同步、jti replay 存储需要 Redis；若 M0 仅使用内存 backend，需降级为“token 仅 exp 校验，不保证单次使用”。
- CSP `frame-ancestors` 与某些企业内网门户的嵌入需求冲突，需 allowlist 可配置。

---

## D6. Admin IAM 边界

### Context

M0 Admin 不虚构用户/角色 CRUD，默认接入企业身份源；前端需要定义权限粒度，但后端仍作最终鉴权。

### Decision

**身份源接入**：

- 默认占位接入企业 OIDC（首选）或 SAML 2.0 / 内部 SSO；不内置本地用户库、角色库、密码登录。
- Admin 前端使用 `@umijs/max` 的 `initialState` + `access` 插件，通过 `src/access.ts` 将 IdP 返回的 groups/roles 映射为前端权限码。
- 后端 `PrincipalAuth` 接收 IdP access token 或 xflow session token，解析出 `Scopes`；`ScopeAuthorizer` 做最终决策。

**前端权限粒度（`src/access.ts`）**：

| 前端权限码 | 适用场景 | 映射到后端 scope（M0 占位） |
|------------|----------|-----------------------------|
| `read` | 查看工作流列表、执行详情 | `workflow.read`、`execution.read` |
| `edit` | 创建/修改草稿、保存节点参数 | `workflow`（即 `workflow.create`） |
| `publish` | 发布工作流版本、调用 invoke | `workflow`、`workflow.invoke` |
| `signal` | 向执行发送 signal | `execution`（即 `execution.signal`） |
| `cancel` | 取消执行 | `execution`（即 `execution.cancel`） |
| `deadletter` | 查看与 replay dead-letter | `deadletter.list`、`deadletter.replay` |

**规则**：

1. 后端 scope 为最终权威；前端 `src/access.ts` 仅控制菜单/按钮可见性，不替代后端鉴权。
2. 后端所有 mutation（`POST /v1/workflows`、`POST /v1/executions/*/signal`、dead-letter replay 等）必须走 B3 authz 路径并写审计日志。
3. Admin 不提供“新增用户”、“新增角色”页面；仅有“当前用户权限查看”和“退出登录”。

### 备选方案

- **本地用户/角色 CRUD**：M0 明确禁止，避免与安全策略冲突。
- **前端自行决定是否放行请求**：拒绝；所有操作必须后端 fail-closed。

### 风险

- 不同企业 IdP 的 groups 字段命名差异大，M0 需要预留 `ACCESS_MAPPING` 环境变量做映射转换。
- `cancel` / `signal` 同属于 `execution` scope，粒度较粗；M0 先用粗粒度，M1 拆分为 `execution.signal` / `execution.cancel`。

---

## D7. 浏览器范围与 npm 发布范围

### Context

M0 需要定义浏览器兼容矩阵和 npm 包发布策略。

### Decision

**浏览器矩阵**：

| 浏览器 | 最低版本 | 说明 |
|--------|----------|------|
| Chrome | 124 | 最近两个主版本 |
| Edge | 124 | Chromium 内核 |
| Firefox | 126 | 最近两个主版本 |
| Safari | 17.4 | macOS / iOS 最近两个主版本 |

- 不考虑 IE。
- 构建目标使用 `browserslist`: `last 2 Chrome versions, last 2 Firefox versions, last 2 Safari versions, last 2 Edge versions, not dead`。
- Monaco Editor 与 React Flow 对 Safari 17+ 支持良好；M0 不兼容旧版 Safari。

**npm 发布范围**:

- 统一使用 `@xflow/*` scope。
- M0 规划包(与计划第 1.2 节一致):

  | 包名 | npm 包 | 职责 | 依赖约束 |
  |------|--------|------|----------|
  | `web/packages/workflow-core` | `@xflow/workflow-core` | `WorkflowDraft`/`WorkflowDefinition`/`WorkflowEditorMetadata`/`ExecutionSnapshot`/`Diagnostic` 类型、YAML/JSON parse/normalize、DAG traversal、spec migration | 禁 React/DOM/ReactFlow/AntD/Umi/HTTP |
  | `web/packages/workflow-renderer` | `@xflow/workflow-renderer` | 基于 `@xyflow/react` 的渲染、overlay、Unknown Node 降级 | 禁 API 调用/保存发布;禁 Umi |
  | `web/packages/workflow-provider` | `@xflow/workflow-provider` | Workflow/Execution/Node Capability Provider 接口 + React Context | 不保存画布实例状态 |
  | `web/packages/workflow-editor` | `@xflow/workflow-editor` | Editor 组件包(实例 store、command stack、palette、property panel、Monaco 接入) | peer React/AntD/ReactFlow |
  | `web/packages/workflow-viewer` | `@xflow/workflow-viewer` | Viewer 组件包(只读展示、execution overlay) | peer React/AntD/ReactFlow |
  | `web/packages/workflow-components` | `@xflow/workflow-components` | 至少两个应用真实复用的工作流业务组件(非通用 Design System) | 不包装 AntD 基础控件 |
  | `web/packages/node-registry` | `@xflow/node-registry` | 前端 `WorkflowNodePlugin` 定义、descriptor/renderer/property panel | 首期静态插件 + 动态 descriptor |
  | `web/packages/api-client` | `@xflow/api-client` | 可替换 `HttpTransport`、AbortSignal、统一错误分类、ETag/409 映射 | 禁读环境变量/全局 singleton/Umi |
  | `web/packages/embed-sdk` | `@xflow/embed-sdk` | `createWorkflowEditor`/`createWorkflowViewer`、postMessage 协议、CSP/origin 校验 | iframe SDK |

  应用(不发布 npm):
  - `web/apps/admin` → Umi + Ant Design Pro,Composition Root。
  - `web/apps/workflow-editor` → Vite 独立编辑器应用。
  - `web/apps/workflow-viewer` → Vite 独立展示器应用。

- 公共 API 稳定前(即 `WorkflowDefinition` TS 类型、Editor/Viewer 组件 props、iframe SDK 协议未冻结前),所有 `@xflow/*` 包保持:
  - `private: true` 或
  - 版本号为 `0.x.y` prerelease(如 `0.1.0-alpha.1`)。
- 使用 `@changesets/cli` 管理版本;pre-release 阶段启用 `changeset pre enter alpha`。

### 备选方案

- **支持 Safari 16 / Chrome 110**：会增加 polyfill 和测试成本，M0 面向内部 Admin，无需兼容旧版；拒绝。
- **无 scope 发布**：易造成命名冲突，且不利于内部 registry 管控；拒绝。

### 风险

- `@xyflow/react` 12.x 对旧浏览器有已知限制；需在 CI 用 Playwright 跑指定版本浏览器矩阵。
- pre-release 版本号可能让下游宿主应用困惑，需在 README 明确“公共 API 未稳定，0.x 可能 break”。

---

## D8. 包依赖图与边界规则

### Context

需要重申计划中的依赖方向，并给出可执行的边界检查方案。

### Decision

**包结构与依赖方向**(与计划第 1.2 节一致):

```
@xflow/workflow-core
        ↑
@xflow/workflow-provider       @xflow/node-registry
        ↑                              ↑
@xflow/api-client          @xflow/workflow-renderer
        ↑                          ↑            ↑
                   @xflow/workflow-editor   @xflow/workflow-viewer
                                  ↑              ↑
                          apps/admin / apps/workflow-editor / apps/workflow-viewer / embed-sdk
```

补充:`@xflow/workflow-components` 是被至少两个应用真实复用的工作流业务组件包;`@xflow/embed-sdk` 是 React 与非 React 宿主可用的 iframe SDK,依赖 `@xflow/workflow-editor`/`@xflow/workflow-viewer`。Viewer 与 Editor 组件包互相不依赖,共同依赖 Renderer。

**禁止规则**:

1. 所有 `@xflow/*` 公共包禁止 import:
   - `@umijs/max`
   - `@ant-design/pro-layout`
   - Admin 专属的 `@/` alias
   - 任何 Umi 运行时 API(`history`、`useModel`、`getInitialState` 等)。
2. `@xflow/workflow-core` 额外禁止 React、DOM、React Flow、Ant Design、Umi、HTTP。
3. `@xflow/workflow-renderer` 不调用 API,不负责保存/发布。
4. `@xflow/workflow-provider` 只定义依赖契约和 React Context,不保存画布实例状态。
5. `@xflow/api-client` 不读取环境变量,不持有 Umi runtime,不在模块加载时创建全局实例。
6. `@xflow/workflow-components` 不是共享 UI/Design System,只接收至少两个应用真实复用的工作流业务组件。
7. 公共组件包禁止 import 应用包;Admin 是 Provider/HTTP/权限/主题/i18n/Node Registry/telemetry 的唯一 Composition Root。

**边界检查实现**:

- **主方案**:`eslint-plugin-import` + `no-restricted-paths`,在 flat config 中为每个公共包配置 `zones`,禁止从 `apps/admin`、`@umijs/max`、`@ant-design/pro-layout` 导入,并对 `@xflow/workflow-core` 追加 React/DOM/HTTP 的 `no-restricted-imports`。
- **辅助方案**:`scripts/check-boundary.mjs` 解析各包 `package.json` 的 `dependencies`,若发现公共包依赖 `@umijs/max` / `@ant-design/pro-layout` 则 CI 失败。
- **CI gate**:`pnpm lint` 和 `pnpm check:boundary` 必须在 PR 合并前通过。

### 备选方案

- **Nx / Turborepo 的 module boundary rules**：功能更强，但 M0 引入新工具成本较高；可在 M1 评估迁移。
- **纯依赖审查（无 ESLint）**：只能阻止直接依赖，无法阻止代码级 import；不推荐作为唯一手段。

### 风险

- Umi 的 `@/` alias 具有传染性，需在公共包 tsconfig / eslint 中显式禁用 `@/` path mapping。
- `eslint-plugin-import` flat config 迁移需要一次性调整；建议在 M0 初始化时即采用 ESLint 9 flat config。

---

## D9. 状态管理与服务端数据策略(ahooks + zustand)

### Context

editor/viewer/admin 需要管理三类状态:① server state(请求/缓存/分页/轮询/变更);② 画布图模型 + undo/redo 命令栈;③ 零散交互态/副作用(debounce/effect/localStorage)。需明确每类用什么工具,避免多套范式并存。关键前提:**server 调用层将由代码生成器产出**(从 OpenAPI 生成 API client + 配套 hooks),这决定了 server state 的 hook 形态要适配生成场景。

### Decision

**引入 ahooks(`3.8.4`,仅 app 层)承担 server state + 交互态;引入 zustand(`5.0.5`,仅 app 层)承担图模型/命令栈;不引入 TanStack Query。**

1. **server state → ahooks `useRequest`**(配合生成的 server 调用代码),**不引入 TanStack Query**。理由:
   - server 调用由代码生成器产出,`useRequest` 的规整返回形态(`{ data, loading, error, run, mutate, refresh }`)非常适合作为生成器的目标模板,生成器可统一封装缓存/重试/错误分类。
   - `useRequest` 是纯 hook,**无全局 store / 无全局 Provider**,符合 ADR D8 约束"api-client 不创建全局实例";TanStack Query 需要全局 `QueryClientProvider`,与该约束冲突。
   - 能力覆盖 editor/viewer/admin 的请求场景:loading/error、手动/自动、轮询、防抖、缓存、refreshDeps、mutate。
2. **画布图模型 + undo/redo 命令栈 → zustand**。理由:
   - 计划 M5.1 要求每条命令 execute/revert、拖动合并历史 —— 这是命令栈 + undo/redo,zustand 有成熟中间件(`zundo`/temporal)与状态快照/回放。
   - 大图性能(计划 M4/M5 验收:100 节点流畅、500 节点可浏览):zustand 的 selector 订阅粒度能精准控制重渲染;ahooks `useReactive`(mutable proxy)是组件局部状态,跨组件共享要套 Context,大图易触发大范围重渲染,不适合图模型。
   - 不可变更新(配 immer)在 React 19 并发模式下比 mutable 安全。
3. **零散交互态/副作用 → ahooks 其余 hooks**(debounce/throttle/useUpdateEffect/useLatest/useLocalStorageState 等),避免为 boolean 建 zustand store。
4. **职责切分(硬规则)**:
   - server state → `useRequest`(生成代码),不另起手写 fetch。
   - 图模型 + 命令栈/undo-redo → zustand;ahooks `useReactive` 不用于图模型。
   - 交互态/副作用 → ahooks。
   - **不引入 TanStack Query**(避免与 useRequest 范式重叠)。
5. **作用域(强制)**:
   - ahooks 与 zustand **仅 app 层**(`apps/admin`、`apps/workflow-editor`、`apps/workflow-viewer`)。
   - **严禁进入 9 个公共包**(`workflow-core` 禁 React 外多余运行时、`api-client` 禁 React、`node-registry` 等)。公共库包内需要的 hooks 用 React 原生或包内最小自实现。
   - `check-boundary.mjs` 把 `ahooks`、`zustand` 加入公共包禁止依赖清单(与 `@umijs/max`、`@ant-design/pro-layout` 同级),CI gate。
6. **版本**:ahooks `3.8.4`、zustand `5.0.5`(均已 React 19 兼容),写死不用 `^`/`latest`。

### 备选方案

- **TanStack Query 做 server state**:功能更强(全局缓存/失效级联/乐观回滚),但需全局 QueryClientProvider,违反 api-client "不创建全局实例"约束;且 server 调用是生成的,`useRequest` 的纯 hook 形态更适配生成器。在生成场景下 TanStack Query 的全局缓存体系优势被抵消。不采用。
- **完全用 ahooks 替代 zustand**:ahooks `useReactive` 无 undo/redo、无 selector 订阅、大图性能差(见 Decision 2),无法满足 M5 命令栈与 100/500 节点性能门禁。不采用。
- **不引入 ahooks,纯 React 19 原生 + zustand**:`useTransition`/`useEffectEvent` 等覆盖部分交互态,但 server state 的 useRequest 形态是生成器目标,且 debounce/effect/localStorage 仍有价值;ahooks 必引。

### 风险

- **useRequest 无全局缓存/失效级联**:跨页面/跨组件共享同一 server 资源时,useRequest 的缓存是组件级。缓解:生成器在 server 调用层统一封装共享缓存(基于 key 的模块级 cache + 手动 invalidate),或对高频共享资源(如 Node Catalog)在 app 层做一层缓存上下文。M6 Admin 列表页实施时需关注,若成为痛点再评估是否对个别资源引入轻量外部 store(用 `useSyncExternalStore`)而非整体换 TanStack Query。
- **乐观更新/409 回滚**:M5 保存/发布需要乐观更新 + 409 冲突回滚,`useMutation` 式能力 useRequest 无原生支持,需在生成层或 app 层手写乐观更新 + 回滚逻辑。M5 实施时明确。
- **版本漂移**:ahooks/zustand 更新频繁,通过锁定版本 + changesets 管理。
- **公共包误引入**:`check-boundary.mjs` + ESLint 兜底拦截。

---

## 待确认项(需用户决策)

1. **企业身份源**:D6 的 OIDC/SAML 具体供应商与 IdP 返回的 groups 字段命名,需在部署前确定;前端 `src/access.ts` 的映射函数(`ACCESS_MAPPING` 环境变量)据此调整。M0 先用 `BearerTokenAuth`/`BearerPrincipalAuth` 占位。
2. **iframe token 降级**:若 M2 持久化层仅内存 backend,`jti` replay 存储需 Redis;M0/M1 可降级为"token 仅 exp 校验,不保证单次使用",M2 接 Redis 后补全。
3. **`Description`/`Nodes[].Notes` 是否纳入 editor metadata 排除**:M0 ADR 暂定排除(与 `assignGraphHash` 对齐),最终在 M2 后端 `runtimeHash` 迁移时定夺。
4. **存量 `definition_hash` 迁移策略**:F0.3 已选定**选项 B**(version bump 自然采用新 hash)。本期 prototype 分支无存量生产数据,不遍历重算;运行时语义未变的存量记录继续可读,新增/升级版本自动使用 `runtime-sha256:v1:`。

---

## 结论

M0 前端工程采用 **React 19 + TypeScript 5.8 + Umi 4 + Ant Design 5 + React Flow 12 + Monaco + Zustand + TanStack Query + Vitest + Playwright** 技术栈,版本全部写死;Editor / Viewer(Vite)同域不同路径独立部署、组件包独立、Admin 用 Umi 脚手架;JSON 为 canonical format,YAML 仅作投影;运行时定义与编辑器元数据分离(`definitionHash` 当前含 Position/UI,M2 迁移至 `runtimeHash` 排除 editor metadata);鉴权走 Bearer token + OIDC 占位 + 短期 iframe JWT;Admin 不接本地用户/角色 CRUD;浏览器支持 Evergreen 最近两个主版本;npm 统一 `@xflow/*` scope(9 个公共包 + 3 个 app)并在 API 稳定前保持 private / pre-release;公共包通过 ESLint + CI 脚本强制与 Umi / ProLayout / Admin alias 隔离。
