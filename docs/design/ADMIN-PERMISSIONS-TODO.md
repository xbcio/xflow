# ADMIN 权限管理 遗留项

**这不是一份待办清单，是一份「设计尚未开始」的记录。**

`web/apps/xflow-admin` 目前有权限相关的代码（`src/access.ts` 的权限位、
`config/routes.ts` 的 `access` 字段），但没有权限模型。那几行是脚手架，不是设计的
产物：它们引用了一套服务端不存在的 scope 词汇，而且计算出来的结果无人消费。

落地这个设计之前，本文件记录三件事：现状是什么（已逐条核实）、为什么现状不会
自己暴露、以及哪些问题必须先有裁定。按「不做会怎样」排序，不按工作量。

---

## 现状（2026-10-01 核实）

| # | 事实 | 位置 |
|---|---|---|
| 1 | 权限位共 3 个，其中 `canViewWorkflows` 与 `canEditWorkflows` 是同一个表达式 | `src/access.ts:18-19`，两者都是 `can('workflow')` |
| 2 | 三个位由 `currentUser.scopes` 派生，而服务端 scope 按**操作**划分、无读写拆分 | `service/apiserver/authz.go` 的 `scopeForOperation` |
| 3 | 路由级 `access` 只声明了 1 条（`/executions`）；`/workflows` 与 `/workflows/:id` 没有 | `config/routes.ts:13` |
| 4 | **权限位没有任何消费方。** umi 的 access 插件依 `config` 算出 `route.unaccessible`，但应用代码不读它，`unAccessible` 也未配置 | `config/config.ts:16` 是 `access: {}`；`src/.umi/` 内 `unaccessible` 仅被计算 |
| 5 | `setToken` 没有调用方。浏览器里没有任何路径能拿到令牌——没有登录页、没有表单、没有环境变量注入 | `src/services/token.ts:25`，全仓仅此一处出现 |
| 6 | 因此 `currentUser` 恒为 `undefined`：`getToken()` 恒为 null 时函数直接返回，连请求都不发 | `src/app.ts:21` |
| 7 | 401 时跳 `/login`，而 `config/routes.ts` 里没有 `/login`，该跳转落到 `*` → 404 | `src/app.ts:8,59` |

由 4 + 6 得到当前的实际行为：**三个权限位恒为假，且无人检查。** 界面看起来正常，
因为「全是 false」和「没人读」互相抵消了。

## 核心问题：两个缺陷互相掩盖

单独看第 6 条（令牌进不了浏览器）或第 4 条（权限位无人消费），都像是无害的未完成。
合起来看则是一个陷阱：

- 只修第 6 条（让令牌进得来）→ 权限位开始有真值，但无人消费，行为不变；
- 只修第 4 条（接上 `unAccessible`）→ 权限位恒假，**每一条声明了 `access` 的路由
  对所有调用者关闭**，且因为第 6 条，连登录后也好不了。

两条必须一起修，且中间态不可交付。任何只做了一半的改动都会表现为「管理台突然坏了」，
而不是「权限还没接上」。

## 必须先裁定，再谈实现

以下问题**都还没有答案**，不要先写代码：

1. **「能力」的粒度是什么？** 是路由可见性、操作可用性（按钮/菜单），还是两者各自
   一套？路由级 `access` 只能表达前者，而 `access.ts` 的三个位暗示的是后者。
2. **前端直接消费服务端 scope，还是先派生一层能力串？** 直接消费省一套需同步维护的
   映射，但服务端的词汇是给授权器用的（`management.runner_pool.write` 这种），
   不是给界面用的；派生则需要指定谁拥有那张映射表。
3. **scope 需要拆出读写吗？** 现在 `workflow` 一个 scope 同时覆盖读与写，所以
   `canEditWorkflows` 目前无法表达「只读操作员」。若产品需要只读角色，这是服务端
   `scopeForOperation` 的改动，不只是前端。
4. **没有权限时看到什么？** 403 页面、隐藏导航、还是编辑器的只读态？`/workflows/:id`
   是全视口工作台（见根 `CLAUDE.md` 的编辑器 UI 约束），它的降级形态尤其需要单独定。
5. **令牌如何进入浏览器？** 登录页、粘贴框、还是接 OIDC？以及配套的刷新、登出、
   401 语义。当前 dev 把令牌注入在代理层（见 `web/apps/xflow-admin/config/proxy.ts`），
   浏览器看不见它——这意味着**在没有令牌入口之前，任何浏览器侧的权限模型都无法在
   dev 里验证**。
6. **需要角色概念吗？** 还是没有角色、scope 就是唯一层级？
7. **跨 namespace 在界面上如何体现？** 服务端的 namespace 是权威且不可跨的
   （`/v1/current-user` 报的就是它），界面是否需要据此裁剪或提示？

## 相关既有条目

以下两条来自一份**被 `.gitignore` 忽略、从未跟踪**的路线图
（`docs/specs/2026-08-24-xflow-completion-roadmap-todo.md`）；该路径不在库里，
工作区一旦清理即失效，故在此逐条转录：

- **身份与登录契约**：决定实现 `/v1/me` 或由前端从受支持的 token/session 入口取得
  主体；明确浏览器凭据存储、刷新、登出和 401 行为；实现真正的 `/login` 页面或删除
  无效跳转；禁止在浏览器 bundle、fixture 或日志中硬编码生产 token。
  *验收：未登录、过期、无权限、跨 namespace 四类场景均有浏览器 E2E。*
- **Admin 接入真实 API 与 Editor**：统一 Admin `antd@6.5.2` 与 Editor `antd@5.24.7`
  的主版本；Workflow 的列表/详情/创建/编辑/保存/执行；Execution 的列表/详情/
  wait-poll/signal/revoke/cancel；Admin 使用 `@xflow/api` 而非第二套手写协议；
  `XFlowEditor` 的保存与运行错误进入统一诊断面板；补齐 loading、empty、error、
  forbidden、not-found 状态。
  *验收：浏览器可走通「登录 → 创建/编辑 → 发布 → 执行 → 查看 → signal/cancel」。*

上述第一条里「决定实现 `/v1/me`」已被取代：`/v1/current-user` 已实现
（`cb12835`，`service/apiserver/module_current_user.go`），返回服务端核验过的
`subject` / `namespace` / `scopes`。剩下的三条——凭据存储与 401 语义、`/login` 的
去留、禁止硬编码 token——仍未做。

## 本次已顺带收口的部分（不属遗留）

- `GET /v1/me` 从未实现且 OpenAPI 未声明，前端却在请求它。已改为 `/v1/current-user`，
  前端类型与该响应体逐字对应（`3f2ce7c`）。
- `src/access.ts` 的注释原先写「权限位由后端下发的 permissions 派生」，而服务端没有
  permissions 这个概念、只有 scopes。已改为按 scope 派生，并把「读写不可区分」这一
  事实写进了注释而不是留给下一个人去发现。
