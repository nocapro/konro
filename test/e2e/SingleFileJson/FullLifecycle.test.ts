import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';

describe('E2E > SingleFileJson > FullLifecycle', () => {
  const dbFilePath = path.join(TEST_DIR, 'e2e_db.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  it('should handle a full data lifecycle: write, read, insert, query, update, delete', async () => {
    // 1. Initialize an empty database file
    let state = db.createEmptyState();
    await db.write(state);
    let fileContent = await fs.readFile(dbFilePath, 'utf-8');
    expect(JSON.parse(fileContent).users.records.length).toBe(0);

    // 2. Read state, insert a user and a post, and write back
    state = await db.read();
    const [s1, user] = db.insert(state, 'users', {
      name: 'E2E User',
      email: 'e2e@test.com',
      age: 42,
      isActive: true,
    });
    const [s2, post] = db.insert(s1, 'posts', {
      title: 'E2E Post',
      content: 'Live from the disk',
      authorId: user.id,
      publishedAt: new Date(),
    });
    await db.write(s2);

    // 3. Read back and verify data integrity
    let readState = await db.read();
    expect(readState.users!.records.length).toBe(1);
    expect(readState.posts!.records.length).toBe(1);
    expect(readState.users!.records[0]?.name).toBe('E2E User');

    // 4. Perform a complex query with relations on the re-read state
    const userWithPosts = await db.query(readState)
      .from('users')
      .where({ id: user.id })
      .with({ posts: true })
      .first();

    expect(userWithPosts).toBeDefined();
    if (userWithPosts) {
      expect(userWithPosts.posts).toBeDefined();
      expect(userWithPosts.posts?.length).toBe(1);
      expect(userWithPosts.posts?.[0]?.title).toBe('E2E Post');
    }

    // 5. Update a record, write the change, and read back to confirm
    const [s3, updatedPosts] = await db.update(readState, 'posts')
      .set({ title: 'Updated E2E Post' })
      .where({ id: post.id });
    expect(updatedPosts.length).toBe(1);
    await db.write(s3);

    let stateAfterUpdate = await db.read();
    const updatedPostFromDisk = db.query(stateAfterUpdate).from('posts').where({ id: post.id }).first();
    expect(updatedPostFromDisk?.title).toBe('Updated E2E Post');

    // 6. Delete a record, write, and confirm it's gone
    const [s4, deletedUsers] = db.delete(stateAfterUpdate, 'users')
      .where({ id: user.id });
    expect(deletedUsers.length).toBe(1);
    await db.write(s4);

    let finalState = await db.read();
    expect(finalState.users!.records.length).toBe(0);
    // The post should also effectively be orphaned, let's check it's still there
    expect(finalState.posts!.records.length).toBe(1);
  });
});
