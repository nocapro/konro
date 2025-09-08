import { konro } from 'konro';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { testSchema, setup, cleanup, TEST_TMP_DIR } from '../test.util';
import { readdir } from 'fs/promises';
import path from 'path';

describe('E2E: Multi-File Strategy (On-Demand)', () => {
  const dir = path.join(TEST_TMP_DIR, 'multi-file-db');

  beforeAll(setup);
  afterAll(cleanup);
  beforeEach(setup);

  it('should perform CRUD operations on demand', async () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      multi: { dir },
      mode: 'on-demand',
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    // Insert user - should create a users.json file
    const user = await db.insert('users', { name: 'On-Demand User', email: 'ondemand@test.com' });
    expect(user.id).toBe(1);

    let files = await readdir(dir);
    expect(files).toContain('users.json');

    // Insert post - should create a posts.json file
    await db.insert('posts', { title: 'On-Demand Post', authorId: user.id });
    files = await readdir(dir);
    expect(files).toContain('posts.json');

    // Query with relation
    const postWithAuthor = await db.query().from('posts').where({ id: 1 }).with({ author: true }).first();
    expect(postWithAuthor).not.toBeNull();
    expect(postWithAuthor?.author?.name).toBe('On-Demand User');

    // Update
    const updatedUsers = await db.update('users').set({ name: 'Updated Name' }).where({ id: 1 });
    expect(updatedUsers).toHaveLength(1);
    const updatedUser = updatedUsers[0];
    expect(updatedUser).toBeDefined();
    if (updatedUser) {
      expect(updatedUser.name).toBe('Updated Name');
    }

    // Verify update with a fresh query
    const freshUser = await db.query().from('users').where({ id: 1 }).first();
    expect(freshUser?.name).toBe('Updated Name');

    // Delete
    const deletedUsers = await db.delete('users').where({ id: 1 });
    expect(deletedUsers).toHaveLength(1);

    const noUser = await db.query().from('users').where({ id: 1 }).first();
    expect(noUser).toBeNull();
  });
});