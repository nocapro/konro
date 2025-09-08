import { konro } from 'konro';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { testSchema, setup, cleanup, TEST_TMP_DIR } from '../test.util';
import { readdir } from 'fs/promises';
import path from 'path';

describe('E2E: Per-Record Strategy (On-Demand)', () => {
  const dir = path.join(TEST_TMP_DIR, 'per-record-db');
  
  beforeAll(setup);
  afterAll(cleanup);
  beforeEach(setup);

  it('should perform CRUD operations on demand', async () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      perRecord: { dir },
      mode: 'on-demand',
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    // Insert user - should create a users directory and record file
    const user = await db.insert('users', { name: 'Record User', email: 'record@test.com' });
    expect(user.id).toBe(1);

    let rootFiles = await readdir(dir);
    expect(rootFiles).toContain('users');
    let userFiles = await readdir(path.join(dir, 'users'));
    expect(userFiles).toContain('1.json');
    expect(userFiles).toContain('_meta.json');

    // Query user
    const foundUser = await db.query().from('users').where({ id: 1 }).first();
    expect(foundUser?.name).toBe('Record User');
    
    // Insert another user
    await db.insert('users', { name: 'Record User 2', email: 'record2@test.com' });
    userFiles = await readdir(path.join(dir, 'users'));
    expect(userFiles).toContain('2.json');

    // Query all users
    const allUsers = await db.query().from('users').all();
    expect(allUsers).toHaveLength(2);

    // Hard delete
    await db.delete('users').where({ id: 1 });
    userFiles = await readdir(path.join(dir, 'users'));
    expect(userFiles).not.toContain('1.json');
    
    const remainingUsers = await db.query().from('users').all();
    expect(remainingUsers).toHaveLength(1);
    const remainingUser = remainingUsers[0];
    expect(remainingUser).toBeDefined();
    if (remainingUser) {
      expect(remainingUser.id).toBe(2);
    }
  });
});