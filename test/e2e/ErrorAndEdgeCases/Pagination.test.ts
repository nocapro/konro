import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > ErrorAndEdgeCases > Pagination', () => {
  const dbFilePath = path.join(TEST_DIR, 'pagination_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

    beforeEach(async () => {
        await ensureTestDir();
        let state = db.createEmptyState();
        const usersToInsert = [];
        for (let i = 1; i <= 100; i++) {
            usersToInsert.push({
                name: `User ${i}`,
                email: `user${i}@test.com`,
                age: 20 + (i % 30),
                isActive: true
            });
        }
        [state] = db.insert(state, 'users', usersToInsert);
        await db.write(state);
    });
    afterEach(cleanup);

  it('should correctly paginate through a large set of records from a file', async () => {
    const state = await db.read();
    expect(state.users!.records.length).toBe(100);

    // Get page 1 (items 1-10)
    const page1 = await db.query(state).from('users').limit(10).offset(0).all();
    expect(page1.length).toBe(10);
    expect(page1[0]?.name).toBe('User 1');
    expect(page1[9]?.name).toBe('User 10');

    // Get page 2 (items 11-20)
    const page2 = await db.query(state).from('users').limit(10).offset(10).all();
    expect(page2.length).toBe(10);
    expect(page2[0]?.name).toBe('User 11');
    expect(page2[9]?.name).toBe('User 20');

    // Get the last page, which might be partial
    const lastPage = await db.query(state).from('users').limit(10).offset(95).all();
    expect(lastPage.length).toBe(5);
    expect(lastPage[0]?.name).toBe('User 96');
    expect(lastPage[4]?.name).toBe('User 100');

    // Get an empty page beyond the end
    const emptyPage = await db.query(state).from('users').limit(10).offset(100).all();
    expect(emptyPage.length).toBe(0);
  });
});
