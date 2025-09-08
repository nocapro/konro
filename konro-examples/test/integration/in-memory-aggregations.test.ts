import { konro } from 'konro';
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../test.util';
import type { DatabaseState } from 'konro';

describe('In-Memory Aggregations', () => {
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: 'dummy.json' },
    mode: 'in-memory',
  });

  const db = konro.createDatabase({ schema: testSchema, adapter });
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    state = db.createEmptyState();
    const [s1] = db.insert(state, 'users', [
      { name: 'Alice', email: 'alice@example.com', age: 30 },
      { name: 'Bob', email: 'bob@example.com', age: 40 },
      { name: 'Charlie', email: 'charlie@example.com', age: 50 },
    ]);
    const [s2] = db.insert(s1, 'posts', [
      { title: 'Post 1', authorId: 1, views: 100 },
      { title: 'Post 2', authorId: 2, views: 150 },
      { title: 'Post 3', authorId: 1, views: 200 },
    ]);
    state = s2;
  });

  it('should count records', () => {
    const result = db.query(state).from('users').aggregate({ total: konro.count() });
    expect(result.total).toBe(3);
  });

  it('should sum a column', () => {
    const result = db.query(state).from('posts').aggregate({ totalViews: konro.sum('views') });
    expect(result.totalViews).toBe(450); // 100 + 150 + 200
  });

  it('should average a column', () => {
    const result = db.query(state).from('users').aggregate({ avgAge: konro.avg('age') });
    expect(result.avgAge).toBe(40); // (30 + 40 + 50) / 3
  });

  it('should find the minimum and maximum of a column', () => {
    const result = db.query(state).from('users').aggregate({
      minAge: konro.min('age'),
      maxAge: konro.max('age'),
    });
    expect(result.minAge).toBe(30);
    expect(result.maxAge).toBe(50);
  });

  it('should handle aggregations on empty sets', () => {
    const result = db.query(state).from('users').where({ name: 'Nonexistent' }).aggregate({
      total: konro.count(),
      avgAge: konro.avg('age'),
      sumViews: konro.sum('views'),
    });
    expect(result.total).toBe(0);
    expect(result.avgAge).toBeNull();
    expect(result.sumViews).toBe(0); // Sum of empty set is 0
  });

  it('should combine aggregations with a where clause', () => {
    // Sum of views for posts by author 1
    const result = db.query(state).from('posts').where({ authorId: 1 }).aggregate({
      totalViews: konro.sum('views'),
      postCount: konro.count(),
    });
    expect(result.totalViews).toBe(300); // 100 + 200
    expect(result.postCount).toBe(2);
  });
});