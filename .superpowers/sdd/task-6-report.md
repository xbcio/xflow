# Task 6 (M1.4) Report

## 变更摘要

- 在根 `Makefile` 新增 `# ── Web (frontend) ──` 段。
- 在 `.github/workflows/ci.yml` 新增并行 `web` job。
- 在 `web/package.json` 增加 `e2e` / `generate` 占位脚本。
- 未修改任何 Go 源码，未改动 `test`/`integration` job。

## Makefile 新增 targets

| target | 命令 |
|--------|------|
| `web-install` | `cd web && pnpm install --frozen-lockfile`（无 lockfile 时回退 `pnpm install`） |
| `web-lint` | `cd web && pnpm lint` |
| `web-typecheck` | `cd web && pnpm typecheck` |
| `web-test` | `cd web && pnpm test` |
| `web-check-boundaries` | `cd web && pnpm check:boundaries` |
| `web-build` | `cd web && pnpm build` |
| `web-e2e` | `cd web && pnpm e2e`（占位，exit 0） |
| `web-generate` | `cd web && pnpm generate`（占位，exit 0） |
| `web-ci` | 聚合：`web-install web-lint web-typecheck web-test web-check-boundaries web-build` |
| `web-all` | `web-ci` 别名 |

`.PHONY` 已同步更新。`all` 仍只依赖 `build`（Go），未引入 web，避免影响现有行为。

## CI 新增 web job

- `name: Web`
- `runs-on: ubuntu-latest`
- `permissions: contents: read`
- 与 `test` / `integration` job 并行，无 `needs`。
- Node 版本：`actions/setup-node@v4`，`node-version-file: web/.nvmrc`（22.15.0）
- pnpm 版本：`pnpm/action-setup@v4`，`version: 10.10.0`，`run_install: false`
- 启用 `cache: pnpm`，`cache-dependency-path: web/pnpm-lock.yaml`
- 执行步骤：Install → Lint → Typecheck → Test → Check boundaries → Build

## 验证输出摘要

### 本地 Makefile 验证

```bash
make web-lint            # OK
make web-typecheck       # OK
make web-test            # 13 passed (15 tests)
make web-check-boundaries # Boundary check passed
make web-build           # 11 successful (turbo cached)
make web-ci              # 全绿
make web-e2e             # e2e deferred to M8
make web-generate        # OpenAPI/TS generation deferred to M2/M3
```

### Go 路径未变

```bash
make build               # bin/server + bin/runner 构建成功
make test                # 全部 package pass
```

### CI YAML 可解析

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
# YAML OK
```

## Commit

`build(ci): add web frontend static gate job` + `build(make): add web-* targets` 合并为一个 commit。

## 遗留问题

- `web-e2e` 与 `web-generate` 为占位实现，待 M2/M3/M8 补充真实逻辑。
- 本地 Node 版本为 v22.18.0，高于 `web/.nvmrc` 锁定的 22.15.0，因此运行时出现 `Unsupported engine` 警告；CI 环境会严格使用 22.15.0，无此警告。
