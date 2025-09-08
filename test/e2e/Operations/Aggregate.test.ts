import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Aggregation', () => {
  const dbFilePath = path.join(TEST_DIR, 'aggregation_test.json');
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
    const usersToInsert = [
      { name: 'User 1', email: 'u1@test.com', age: 20, isActive: true },
      { name: 'User 2', email: 'u2@test.com', age: 25, isActive: true },
      { name: 'User 3', email: 'u3@test.com', age: 30, isActive: false },
      { name: 'User 4', email: 'u4@test.com', age: 35, isActive: true },
      { name: 'User 5', email: 'u5@test.com', age: 40, isActive: false },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should correctly calculate count, sum, avg, min, and max', async () => {
    const state = await db.read();

    const stats = db.query(state)
      .from('users')
      .aggregate({
        totalUsers: konro.count(),
        totalAge: konro.sum('age'),
        averageAge: konro.avg('age'),
        minAge: konro.min('age'),
        maxAge: konro.max('age'),
      });

    expect(stats.totalUsers).toBe(5);
    expect(stats.totalAge).toBe(20 + 25 + 30 + 35 + 40); // 150
    expect(stats.averageAge).toBe(150 / 5); // 30
    expect(stats.minAge).toBe(20);
    expect(stats.maxAge).toBe(40);
  });

  it('should correctly calculate aggregations with a where clause', async () => {
    const state = await db.read();

    const stats = db.query(state)
      .from('users')
      .where({ isActive: true })
      .aggregate({
        activeUsers: konro.count(),
        totalAgeActive: konro.sum('age'),
      });

    expect(stats.activeUsers).toBe(3);
    expect(stats.totalAgeActive).toBe(20 + 25 + 35); // 80
  });

  it('should handle aggregations on empty sets', async () => {
    const state = await db.read();

    const stats = db.query(state)
      .from('users')
      .where({ name: 'Nonexistent' })
      .aggregate({
        count: konro.count(),
        sum: konro.sum('age'),
        avg: konro.avg('age'),
        min: konro.min('age'),
        max: konro.max('age'),
      });

    expect(stats.count).toBe(0);
    expect(stats.sum).toBe(0); // sum of empty set is 0
    expect(stats.avg).toBeNull();
    expect(stats.min).toBeNull();
    expect(stats.max).toBeNull();
  });
});