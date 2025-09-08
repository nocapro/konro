import { konro } from 'konro';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { testSchema, setup, cleanup, TEST_TMP_DIR } from '../test.util';
import { readFile } from 'fs/promises';
import path from 'path';
import jsyaml from 'js-yaml';

describe('E2E: Single-File Strategy', () => {
  beforeAll(setup);
  afterAll(cleanup);
  beforeEach(setup); // Clean tmp dir before each test

  const formats = ['json', 'yaml'] as const;

  for (const format of formats) {
    it(`should write and read a database using ${format} format`, async () => {
      const filepath = path.join(TEST_TMP_DIR, `db.${format}`);
      const adapter = konro.createFileAdapter({
        format,
        single: { filepath },
        mode: 'in-memory',
      });
      const db = konro.createDatabase({ schema: testSchema, adapter });
      
      let state = db.createEmptyState();
      const [nextState] = db.insert(state, 'users', { name: 'E2E User', email: 'e2e@test.com' });

      await db.write(nextState);

      // Verify file content
      const fileContent = await readFile(filepath, 'utf-8');
      expect(fileContent).not.toBeNull();
      const parsedContent = format === 'json' ? JSON.parse(fileContent) : jsyaml.load(fileContent);
      expect(parsedContent.users.records[0].name).toBe('E2E User');

      // Read the state back
      const readState = await db.read();
      expect(readState.users).toBeDefined();
      if (readState.users) {
        expect(readState.users.records).toHaveLength(1);
        const user = readState.users.records[0];
        expect(user).toBeDefined();
        if (user) {
          expect(user.name).toBe('E2E User');
        }
        expect(readState.users.meta.lastId).toBe(1);
      }
    });
  }
});