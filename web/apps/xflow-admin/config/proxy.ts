/**
 * 开发代理。仅开发期生效，不进产物——生产环境由部署层反代。
 *
 * target 指向本地 apiserver。契约见 api/openapi/xflow-v1.yaml，
 * 全部端点在 /v1 前缀下。
 */
export const proxy = {
  '/v1': {
    target: 'http://127.0.0.1:8080',
    changeOrigin: true,
  },
};
