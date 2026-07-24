# Task 4.1 Report — cmd/server Redis HA flags + apiserver RedisConfig wiring

## 变更摘要

- `cmd/server/main.go`：新增 Redis HA flag（`--redis-mode`、`--redis-sentinel-master`、
  `--redis-cluster-addrs`、`--redis-sentinel-addrs`、`--redis-username`、
  `--redis-password`、`--redis-db`、`--redis-tls`）。新增 `buildRedisConfig` 函数，
  负责把 CLI flag 组装成 `distributed.RedisConfig`，对 sentinel/cluster 做 fail-closed
  校验，并默认保持 `--redis` 单地址路径以兼容现有部署。
- `service/apiserver/apiserver.go`：`Config` 新增 `RedisConfig *distributed.RedisConfig`
  字段，保留 `RedisAddr string` 兼容字段。`buildControlPlane` 优先使用 `RedisConfig`
  （通过 `distributed.WithRedisConfig`），否则回退到 `RedisAddr` 路径；两者皆空时走
  in-memory backend。
- 测试：新增 flag 解析、fail-closed 校验、`RedisConfig` 注入路径、兼容路径、内存回退
  等单测。

## 兼容性与安全性

- 仅传 `--redis <addr>`（无 HA flag）时，`buildRedisConfig` 返回 `nil`，`apiserver.Config`
  只设 `RedisAddr`，行为与修改前完全一致。
- `--redis ""`（且未提供 HA topology flag）仍判定为 in-memory backend。
- sentinel 模式无 `--redis-sentinel-master` 时 `buildRedisConfig` 返回 error，启动失败。
- cluster 模式无 `--redis-cluster-addrs` 时同样 fail-closed。
- 启动日志打印 redis 配置时只输出 mode/addr 数量/master/tls/db，不输出 password。
- 未修改 `backend/distributed` 包本身。

## 测试结果

```
go build ./...
go vet ./...
go test ./cmd/server/... ./service/apiserver/... -race -count=1
# ok  	github.com/xbcio/xflow/cmd/server	2.334s
# ok  	github.com/xbcio/xflow/service/apiserver	6.186s
```

## 环境门控

- 真实 sentinel/cluster 启动验证仍为 `[ENV-GATED]`，本任务仅完成代码与单测。

## Commit

`feat(server): add Redis HA mode flags and wire RedisConfig through apiserver`

## Fix

针对审查反馈的三处修复：

### 1. sentinel 独立凭据 flag（Important）

- 在 `cmd/server/main.go` 新增 `--redis-sentinel-username` / `--redis-sentinel-password` flag（默认空）。
- `buildRedisConfig` 中 sentinel 凭据优先使用专用 flag；未提供时回退到 `--redis-username` / `--redis-password`，保持共享凭据的向后兼容。
- 密码不进入日志。

### 2. 复用 `RedisConfig.validate()`（Minor 1）

- 在 `backend/distributed/redisconfig.go` 补充 single 模式也需要至少一个地址的校验（之前只 sentinel/cluster 检查）。
- 新增导出的 `Validate()` 包装方法，使 `cmd/server` 可以复用统一的 fail-closed 校验。
- `buildRedisConfig` 构造完 `RedisConfig` 后调用 `rc.Validate()`，移除原先与 `validate()` 重复的检查逻辑。
- 保留 legacy 行为：仅 `--redis` 时返回 `nil`，`--redis ""` 且无 HA flag 时仍走 in-memory。

### 3. `--memory` 优先级与日志（Minor 2）

- `runServer` 中 `--memory` 显式优先：当 `cfg.memory` 为 true 且存在 redis 配置时，清空 `RedisConfig` / `RedisAddr`，确保实际走 in-memory backend。
- 同时打印 warning：`memory flag set, ignoring redis configuration`，避免“log 说 in-memory 实际走 distributed”的误导。

### 新增/更新测试

- `TestBuildRedisConfigSentinelUsesDedicatedCreds`
- `TestBuildRedisConfigSentinelFallsBackToMasterCreds`
- `TestParseServerConfigSupportsSentinelAuthFlags`
- 更新 `backend/distributed/redisconfig_test.go` 中 single 空地址用例为期望失败。

### 测试结果

```
go build ./...
go vet ./cmd/server/... ./service/apiserver/...
go test ./cmd/server/... ./service/apiserver/... ./backend/distributed/... -race -count=1
# ok  	github.com/xbcio/xflow/cmd/server	2.979s
# ok  	github.com/xbcio/xflow/service/apiserver	5.442s
# ok  	github.com/xbcio/xflow/backend/distributed	5.904s
...
```

### Commit

`fix(server): expose sentinel auth flags, reuse RedisConfig.validate, clarify memory flag`
