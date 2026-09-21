/**
 * 开发代理。仅开发期生效，不进产物——生产环境由部署层反代。
 *
 * target 默认指向本地 apiserver；开发时可用 XFLOW_API_TARGET 覆盖端口。
 * 契约见 api/openapi/xflow-v1.yaml，全部端点在 /v1 前缀下。
 */
export const proxy = {
  '/v1': {
    target: process.env.XFLOW_API_TARGET ?? 'http://127.0.0.1:8080',
    changeOrigin: true,
  },
};
