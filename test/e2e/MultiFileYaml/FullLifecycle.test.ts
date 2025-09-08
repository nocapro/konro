import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';

describe('E2E > MultiFileYaml > FullLifecycle', () => {
  const dbDirPath = path.join(TEST_DIR, 'e2e_yaml_db');
  const adapter = konro.createFileAdapter({
    format: 'yaml',
    multi: { dir: dbDirPath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  it('should handle a full data lifecycle across multiple YAML files', async () => {
    // 1. Initialize empty database files
    let state = db.createEmptyState();
    await db.write(state);

    // Check that empty files are created
    const usersFilePath = path.join(dbDirPath, 'users.yaml');
    let usersFileContent = await fs.readFile(usersFilePath, 'utf-8');
    expect(yaml.load(usersFileContent)).toEqual({ records: [], meta: { lastId: 0 } });

    // 2. Insert data and write to disk
    const [s1, user] = db.insert(state, 'users', { name: 'E2E Yaml', email: 'yaml.e2e@test.com', age: 50, isActive: true });
    const [s2] = db.insert(s1, 'posts', { title: 'YAML Post', content: '...', authorId: user.id, publishedAt: new Date() });
    await db.write(s2);

    // 3. Read back and verify integrity from separate files
    const readState = await db.read();
    expect(readState.users!.records.length).toBe(1);
    expect(readState.posts!.records.length).toBe(1);
    expect(readState.users!.records[0]?.id).toBe(user.id);

    // 4. Query with relations
    const userWithPosts = db.query(readState).from('users').where({ id: user.id }).with({ posts: true }).first();
    expect(userWithPosts).toBeDefined();
    if (userWithPosts) {
      expect(userWithPosts.posts).toBeDefined();
      expect(userWithPosts.posts?.length).toBe(1);
      expect(userWithPosts.posts?.[0]?.title).toBe('YAML Post');
    }

    // 5. Update and write
    const [s3] = await db.update(readState, 'users').set({ name: 'Updated Yaml User' }).where({ id: user.id });
    await db.write(s3);
    const stateAfterUpdate = await db.read();
    expect(stateAfterUpdate.users!.records[0]?.name).toBe('Updated Yaml User');

    // 6. Delete and write
    const [s4] = await db.delete(stateAfterUpdate, 'posts').where({ authorId: user.id });
    await db.write(s4);
    const finalState = await db.read();
    expect(finalState.posts!.records.length).toBe(0);
    expect(finalState.users!.records.length).toBe(1);
  });
});
