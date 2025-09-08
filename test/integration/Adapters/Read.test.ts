import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';
import { KonroStorageError } from '../../../src/utils/error.util';

describe('Integration > Adapters > Read', () => {

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  describe('SingleFileJson', () => {
    const dbFilePath = path.join(TEST_DIR, 'read_test.json');
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: dbFilePath },
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    it('should correctly read and parse a single JSON file', async () => {
      const state = db.createEmptyState();
      state.users.records.push({ id: 1, name: 'Reader', email: 'reader@test.com', age: 30, isActive: true });
      state.users.meta.lastId = 1;
      await fs.writeFile(dbFilePath, JSON.stringify(state, null, 2));

      const readState = await db.read();
      expect(readState.users.records.length).toBe(1);
      expect(readState.users.records[0]?.name).toBe('Reader');
      expect(readState.users.meta.lastId).toBe(1);
    });

    it('should return an empty state if the file does not exist', async () => {
      const readState = await db.read();
      expect(readState).toEqual(db.createEmptyState());
    });

    it('should throw KonroStorageError for a corrupt JSON file', async () => {
      await fs.writeFile(dbFilePath, '{ "users": { "records": ['); // Invalid JSON
      await expect(db.read()).rejects.toThrow(KonroStorageError);
    });
  });

  describe('MultiFileYaml', () => {
    const dbDirPath = path.join(TEST_DIR, 'read_yaml_test');
    const adapter = konro.createFileAdapter({
      format: 'yaml',
      multi: { dir: dbDirPath },
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    it('should correctly read and parse multiple YAML files', async () => {
      const state = db.createEmptyState();
      state.users.records.push({ id: 1, name: 'YamlReader', email: 'yaml@test.com', age: 31, isActive: true });
      state.users.meta.lastId = 1;
      state.posts.records.push({ id: 1, title: 'Yaml Post', content: '...', authorId: 1, publishedAt: new Date() });
      state.posts.meta.lastId = 1;

      await fs.mkdir(dbDirPath, { recursive: true });
      await fs.writeFile(path.join(dbDirPath, 'users.yaml'), yaml.dump({ records: state.users.records, meta: state.users.meta }));
      await fs.writeFile(path.join(dbDirPath, 'posts.yaml'), yaml.dump({ records: state.posts.records, meta: state.posts.meta }));
      
      const readState = await db.read();
      expect(readState.users.records.length).toBe(1);
      expect(readState.users.records[0]?.name).toBe('YamlReader');
      expect(readState.posts.records.length).toBe(1);
      expect(readState.posts.records[0]?.title).toBe('Yaml Post');
      expect(readState.tags.records.length).toBe(0); // Ensure non-existent files are handled
    });

    it('should return an empty state if the directory does not exist', async () => {
      const readState = await db.read();
      expect(readState).toEqual(db.createEmptyState());
    });
  });
});