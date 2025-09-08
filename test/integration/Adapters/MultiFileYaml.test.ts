import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';

describe('Integration > Adapters > MultiFileYaml', () => {
  const dbDirPath = path.join(TEST_DIR, 'yaml_db');
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

  it('should correctly write each table to a separate YAML file', async () => {
    let state = db.createEmptyState();
    [state] = db.insert(state, 'users', {
      name: 'YAML User',
      email: 'yaml@test.com',
      age: 44,
    });
    [state] = db.insert(state, 'posts', {
      title: 'YAML Post',
      content: 'Content here',
      authorId: 1,
    });

    await db.write(state);

    const usersFilePath = path.join(dbDirPath, 'users.yaml');
    const postsFilePath = path.join(dbDirPath, 'posts.yaml');

    const usersFileExists = await fs.access(usersFilePath).then(() => true).catch(() => false);
    const postsFileExists = await fs.access(postsFilePath).then(() => true).catch(() => false);
    expect(usersFileExists).toBe(true);
    expect(postsFileExists).toBe(true);

    const usersFileContent = await fs.readFile(usersFilePath, 'utf-8');
    const postsFileContent = await fs.readFile(postsFilePath, 'utf-8');

    const parsedUsers = yaml.load(usersFileContent) as { records: unknown[], meta: unknown };
    const parsedPosts = yaml.load(postsFileContent) as { records: unknown[], meta: unknown };

    expect(parsedUsers.records.length).toBe(1);
    expect((parsedUsers.records[0] as { name: string }).name).toBe('YAML User');
    expect((parsedUsers.meta as { lastId: number }).lastId).toBe(1);

    expect(parsedPosts.records.length).toBe(1);
    expect((parsedPosts.records[0] as { title: string }).title).toBe('YAML Post');
    expect((parsedPosts.meta as { lastId: number }).lastId).toBe(1);
  });

  it('should correctly serialize and deserialize dates', async () => {
    let state = db.createEmptyState();
    const testDate = new Date('2023-10-27T10:00:00.000Z');

    [state] = db.insert(state, 'posts', {
      title: 'Dated Post',
      content: '...',
      authorId: 1,
      publishedAt: testDate,
    });

    await db.write(state);

    const readState = await db.read();

    expect(readState.posts.records[0]?.publishedAt).toBeInstanceOf(Date);
    expect((readState.posts.records[0]?.publishedAt as Date).getTime()).toBe(testDate.getTime());
  });
});
