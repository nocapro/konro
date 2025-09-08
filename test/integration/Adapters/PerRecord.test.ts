import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir, uuidTestSchema } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';
import { KonroError, KonroStorageError } from '../../../src/utils/error.util';
import type { InMemoryDbContext, OnDemandDbContext } from '../../../src/db';

describe('Integration > Adapters > PerRecord', () => {
  const dbDirPath = path.join(TEST_DIR, 'per_record_db');

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  describe('Initialization', () => {
    it('should successfully create a per-record adapter', () => {
      const adapter = konro.createFileAdapter({
        format: 'json',
        perRecord: { dir: dbDirPath },
      });
      expect(adapter).toBeDefined();
      expect(adapter.options.perRecord).toEqual({ dir: dbDirPath });
    });

    it('should throw an error for unsupported formats like "csv"', () => {
      expect(() => {
        konro.createFileAdapter({
          format: 'csv',
          perRecord: { dir: dbDirPath },
        });
      }).toThrow(KonroError({ code: 'E105' }));
    });
  });

  describe('In-Memory Mode (JSON)', () => {
    let db: InMemoryDbContext<typeof testSchema>;
    const adapter = konro.createFileAdapter({
      format: 'json',
      perRecord: { dir: dbDirPath },
    });

    beforeEach(() => {
      db = konro.createDatabase({ schema: testSchema, adapter });
    });

    it('should write each record to a separate file and a meta file', async () => {
      let state = db.createEmptyState();
      [state] = db.insert(state, 'users', { name: 'Record User', email: 'rec@test.com', age: 33 });
      [state] = db.insert(state, 'posts', { title: 'Record Post', content: '...', authorId: 1 });

      await db.write(state);

      const usersDir = path.join(dbDirPath, 'users');
      const postsDir = path.join(dbDirPath, 'posts');
      
      const userRecordPath = path.join(usersDir, '1.json');
      const userMetaPath = path.join(usersDir, '_meta.json');
      const postRecordPath = path.join(postsDir, '1.json');
      const postMetaPath = path.join(postsDir, '_meta.json');

      const userRecordContent = JSON.parse(await fs.readFile(userRecordPath, 'utf-8'));
      const userMetaContent = JSON.parse(await fs.readFile(userMetaPath, 'utf-8'));
      const postRecordContent = JSON.parse(await fs.readFile(postRecordPath, 'utf-8'));
      const postMetaContent = JSON.parse(await fs.readFile(postMetaPath, 'utf-8'));

      expect(userRecordContent.name).toBe('Record User');
      expect(userMetaContent.lastId).toBe(1);
      expect(postRecordContent.title).toBe('Record Post');
      expect(postMetaContent.lastId).toBe(1);
    });

    it('should delete record files that are no longer in the state', async () => {
      let state = db.createEmptyState();
      [state] = db.insert(state, 'users', { name: 'To Be Deleted', email: 'del@test.com', age: 40 });
      await db.write(state);
      
      const userRecordPath = path.join(dbDirPath, 'users', '1.json');
      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(true);

      [state] = db.delete(state, 'users').where({ id: 1 });
      await db.write(state);

      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(false);
    });

    it('should read records from individual files to build the state', async () => {
      // Manually create files
      const usersDir = path.join(dbDirPath, 'users');
      await fs.mkdir(usersDir, { recursive: true });
      await fs.writeFile(path.join(usersDir, '1.json'), JSON.stringify({ id: 1, name: 'Manual User', email: 'man@test.com', age: 50, isActive: true }));
      await fs.writeFile(path.join(usersDir, '_meta.json'), JSON.stringify({ lastId: 1 }));
      
      const state = await db.read();
      
      expect(state.users.records.length).toBe(1);
      expect(state.users.records[0]?.name).toBe('Manual User');
      expect(state.users.meta.lastId).toBe(1);
      expect(state.posts.records.length).toBe(0);
    });
    
    it('should derive lastId from record files if meta file is missing', async () => {
        const usersDir = path.join(dbDirPath, 'users');
        await fs.mkdir(usersDir, { recursive: true });
        await fs.writeFile(path.join(usersDir, '2.json'), JSON.stringify({ id: 2, name: 'User 2', email: 'u2@test.com', age: 50, isActive: true }));
        await fs.writeFile(path.join(usersDir, '5.json'), JSON.stringify({ id: 5, name: 'User 5', email: 'u5@test.com', age: 50, isActive: true }));

        const state = await db.read();
        expect(state.users.meta.lastId).toBe(5);
    });

    it('should throw KonroStorageError for a corrupt record file', async () => {
      const usersDir = path.join(dbDirPath, 'users');
      await fs.mkdir(usersDir, { recursive: true });
      await fs.writeFile(path.join(usersDir, '1.json'), '{ "id": 1, "name": "Corrupt"'); // Invalid JSON
      
      await expect(db.read()).rejects.toThrow(KonroStorageError);
    });
  });

  describe('On-Demand Mode (YAML)', () => {
    let db: OnDemandDbContext<typeof testSchema>;
    
    beforeEach(() => {
        const adapter = konro.createFileAdapter({
            format: 'yaml',
            mode: 'on-demand',
            perRecord: { dir: dbDirPath },
        });
        db = konro.createDatabase({ schema: testSchema, adapter });
    });

    it('should insert a record and create its file and update meta', async () => {
      const user = await db.insert('users', { name: 'OnDemand Record', email: 'odr@test.com', age: 25 });
      
      const userRecordPath = path.join(dbDirPath, 'users', `${user.id}.yaml`);
      const userMetaPath = path.join(dbDirPath, 'users', '_meta.json');

      const recordContent = yaml.load(await fs.readFile(userRecordPath, 'utf-8')) as any;
      const metaContent = JSON.parse(await fs.readFile(userMetaPath, 'utf-8'));

      expect(recordContent.name).toBe('OnDemand Record');
      expect(metaContent.lastId).toBe(1);
    });

    it('should update a record file', async () => {
      const user = await db.insert('users', { name: 'Update Me', email: 'upd@test.com', age: 35 });
      await db.update('users').set({ name: 'Updated Name' }).where({ id: user.id });

      const userRecordPath = path.join(dbDirPath, 'users', `${user.id}.yaml`);
      const recordContent = yaml.load(await fs.readFile(userRecordPath, 'utf-8')) as any;
      
      expect(recordContent.name).toBe('Updated Name');
    });

    it('should delete a record file', async () => {
      const user = await db.insert('users', { name: 'Delete Me', email: 'del@test.com', age: 45 });
      const userRecordPath = path.join(dbDirPath, 'users', `${user.id}.yaml`);
      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(true);

      await db.delete('users').where({ id: user.id });
      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(false);
    });

    it('should query with relations by reading multiple tables', async () => {
        const user = await db.insert('users', { name: 'Author', email: 'author@test.com', age: 35 });
        await db.insert('posts', { title: 'Post by Author', content: '...', authorId: user.id });
        
        const userWithPosts = await db.query().from('users').where({ id: user.id }).with({ posts: true }).first();
        
        expect(userWithPosts).toBeDefined();
        expect(userWithPosts?.posts?.length).toBe(1);
        expect(userWithPosts?.posts?.[0]?.title).toBe('Post by Author');
    });
  });

  describe('ID Handling', () => {
    it('should generate UUIDs for filenames and record IDs', async () => {
        const adapter = konro.createFileAdapter({
            format: 'json',
            mode: 'on-demand',
            perRecord: { dir: dbDirPath },
        });
        const db = konro.createDatabase({ schema: uuidTestSchema, adapter });

        const user = await db.insert('uuid_users', { name: 'UUID User' });
        
        expect(typeof user.id).toBe('string');
        const userRecordPath = path.join(dbDirPath, 'uuid_users', `${user.id}.json`);
        expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(true);
        
        const recordContent = JSON.parse(await fs.readFile(userRecordPath, 'utf-8'));
        expect(recordContent.id).toBe(user.id);
        expect(recordContent.name).toBe('UUID User');
    });

    it('on-demand insert should not derive lastId from existing files', async () => {
        // Manually create a file with ID 5, but no meta file
        const usersDir = path.join(dbDirPath, 'users');
        await fs.mkdir(usersDir, { recursive: true });
        await fs.writeFile(path.join(usersDir, '5.json'), JSON.stringify({ id: 5, name: 'Existing User', email: 'ex@test.com', age: 55, isActive: true }));
        
        const adapter = konro.createFileAdapter({ format: 'json', mode: 'on-demand', perRecord: { dir: dbDirPath } });
        const db = konro.createDatabase({ schema: testSchema, adapter });
        
        // Inserting should start from ID 1 because _meta.json doesn't exist
        const newUser = await db.insert('users', { name: 'New User', email: 'new@test.com', age: 22 });
        expect(newUser.id).toBe(1);
        
        const metaContent = JSON.parse(await fs.readFile(path.join(usersDir, '_meta.json'), 'utf-8'));
        expect(metaContent.lastId).toBe(1);
    });
  });
});