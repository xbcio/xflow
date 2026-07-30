/**
 * 访问令牌存放。
 *
 * 内存为主、sessionStorage 为副本（供刷新页面后恢复）。
 * 刻意不用 localStorage：它跨标签页与浏览器会话持久，XSS 一旦得手即长期泄露。
 * 令牌也绝不放进 URL 参数——会进浏览器历史、Referer 与服务端访问日志。
 */
const STORAGE_KEY = 'xflow.token';

let memoryToken: string | null = null;

export function getToken(): string | null {
  if (memoryToken !== null) {
    return memoryToken;
  }
  try {
    memoryToken = sessionStorage.getItem(STORAGE_KEY);
  } catch {
    // sessionStorage 在隐私模式或被策略禁用时会抛错，此时退化为纯内存
    memoryToken = null;
  }
  return memoryToken;
}

export function setToken(token: string): void {
  memoryToken = token;
  try {
    sessionStorage.setItem(STORAGE_KEY, token);
  } catch {
    // 忽略：内存副本仍然有效，仅失去刷新后恢复能力
  }
}

export function clearToken(): void {
  memoryToken = null;
  try {
    sessionStorage.removeItem(STORAGE_KEY);
  } catch {
    // 忽略
  }
}
