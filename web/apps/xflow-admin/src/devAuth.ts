/**
 * 开发期代理认证标志。
 *
 * config/proxy.ts 在 dev server 内部替 /v1 请求补 Bearer，浏览器不必持有令牌。
 * config/config.ts 仅在 `max dev`（NODE_ENV=development）且代理解析到令牌时把
 * 本标志 define 为 true；生产构建恒为 false。令牌值本身从不进入前端产物。
 *
 * umi 的 define 会把 `process.env.XFLOW_DEV_PROXY_AUTH` 整个替换成字面量
 * true / false；未经 define 的消费方（vitest）读到的是环境变量字符串，所以两种
 * 形态都要认。只暴露「代理会替我认证」这一个布尔事实，不读取任何凭据。
 */
export function hasDevProxyAuth(): boolean {
  const flag: unknown = process.env.XFLOW_DEV_PROXY_AUTH;
  return flag === true || flag === 'true';
}
