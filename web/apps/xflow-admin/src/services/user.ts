import { request } from '@umijs/max';

/** 当前登录用户。permissions 为后端下发的权限位标识集合，access.ts 据此派生权限。 */
export interface CurrentUser {
  id: string;
  name: string;
  avatar?: string;
  permissions: string[];
}

export async function fetchCurrentUser(): Promise<CurrentUser> {
  return request<CurrentUser>('/v1/me', { method: 'GET' });
}
