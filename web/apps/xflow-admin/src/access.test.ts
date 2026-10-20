import { describe, expect, it } from 'vitest';

import access from './access';

const state = (scopes: string[]) => ({
  currentUser: { subject: 'subject-a', namespace: 'ns-a', scopes },
  fetchUserInfo: async () => undefined,
});

describe('workflow access bits', () => {
  it('lets the pre-B5 workflow scope both view and edit', () => {
    const can = access(state(['workflow']));

    expect(can.canViewWorkflows).toBe(true);
    expect(can.canEditWorkflows).toBe(true);
  });

  it('lets the read-only workflow.read scope view but not edit', () => {
    const can = access(state(['workflow.read']));

    expect(can.canViewWorkflows).toBe(true);
    expect(can.canEditWorkflows).toBe(false);
  });

  it('denies both without a workflow scope, including an absent session', () => {
    const can = access(state(['execution']));

    expect(can.canViewWorkflows).toBe(false);
    expect(can.canEditWorkflows).toBe(false);
    expect(access(undefined).canViewWorkflows).toBe(false);
    expect(access(undefined).canEditWorkflows).toBe(false);
  });
});
